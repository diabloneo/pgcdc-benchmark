package main

import (
	"context"
	"fmt"
	"gorm.io/gorm/clause"
	"log"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgproto3/v2"
	"github.com/pkg/errors"

	"pgcdc-benchmark/model"
)

// NewPgLogicalReplicationClient creates a new Postgres logical replication client.
func NewPgLogicalReplicationClient(name string, arg *model.ConnectionArg) *pgLogicalReplicationClient {
	client := &pgLogicalReplicationClient{
		name:           name,
		connArg:        arg.Copy(),
		replyRequestCh: make(chan struct{}, 1),
		msgBuffer:      make([]pglogrepl.Message, 0, 4),
		relationMap:    make(map[uint32]string),
	}
	return client
}

// pgLogicalReplicationClient receive Postgres's logical replication message and forward
// to Kafka.
type pgLogicalReplicationClient struct {
	name string

	connArg *model.ConnectionArg
	conn    *pgconn.PgConn

	walPos pglogrepl.LSN

	walPosCh       chan pglogrepl.LSN
	replyRequestCh chan struct{}

	msgBuffer   []pglogrepl.Message
	relationMap map[uint32]string
}

func (c *pgLogicalReplicationClient) publicationName() string {
	return fmt.Sprintf("app_%s", c.name)
}

func (c *pgLogicalReplicationClient) replicationSlotName() string {
	return fmt.Sprintf("app_%s", c.name)
}

func (c *pgLogicalReplicationClient) prepareReplication() (err error) {

	db := model.NewDB(envID)
	err = db.Exec("CREATE PUBLICATION ? FOR ALL TABLES", clause.Table{Name: c.publicationName()}).Error
	if err != nil {
		return errors.Wrap(err, "create publication")
	}

	err = db.Exec("SELECT pg_create_logical_replication_slot(?, 'pgoutput');", c.replicationSlotName()).Error
	if err != nil {
		return errors.Wrap(err, "create replication slot")
	}
	log.Printf("App %s is ready", c.name)

	return nil
}

func (c *pgLogicalReplicationClient) connect() (err error) {
	c.connArg.Replication = "database"

	c.conn, err = pgconn.Connect(context.Background(), c.connArg.DSN())
	if err != nil {
		return errors.Wrapf(err, "connect to postgres %s", c.connArg.Host)
	}
	log.Printf("Connected to %s in logical replicaton mode", c.connArg.Host)

	return nil
}

func (c *pgLogicalReplicationClient) subscribeToPublication() (err error) {
	resultReader := c.conn.Exec(context.Background(),
		fmt.Sprintf("SELECT restart_lsn FROM pg_replication_slots WHERE slot_name='%s'",
			c.replicationSlotName()),
	)
	results, err := resultReader.ReadAll()
	if err != nil {
		return errors.Wrapf(err, "get carrier replication slot")
	}
	for _, result := range results {
		if len(result.Rows) == 0 {
			continue
		}
		restartLSNIndex := -1
		for i, f := range result.FieldDescriptions {
			if string(f.Name) == "restart_lsn" {
				restartLSNIndex = i
			}
		}
		if restartLSNIndex == -1 {
			continue
		}
		c.walPos, err = pglogrepl.ParseLSN(string(result.Rows[0][restartLSNIndex]))
		if err != nil {
			return errors.Wrapf(err, "invalid restart_lsn %s", string(result.Rows[0][restartLSNIndex]))
		}
		break
	}
	if c.walPos == 0 {
		return errors.Errorf("replication slot %s not found", c.replicationSlotName())
	}

	log.Printf("Start logical replication from WAL position: %s", c.walPos)

	err = pglogrepl.StartReplication(context.Background(), c.conn,
		c.replicationSlotName(),
		c.walPos,
		pglogrepl.StartReplicationOptions{
			Mode: pglogrepl.LogicalReplication,
			PluginArgs: []string{
				"proto_version '1'",
				fmt.Sprintf("publication_names '%s'", c.publicationName()),
			},
		},
	)
	if err != nil {
		return errors.Wrapf(err, "START_REPLICATION")
	}

	return nil
}

func (c *pgLogicalReplicationClient) processCopyDataMessage(msg *pgproto3.CopyData) (err error) {
	switch msg.Data[0] {
	case pglogrepl.PrimaryKeepaliveMessageByteID:
		log.Printf("Receive primary keepalive message")
		pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
		if err != nil {
			return errors.Wrapf(err, "parse primary keepalive message")
		}
		if pkm.ReplyRequested {
			select {
			case c.replyRequestCh <- struct{}{}:
			default:
			}
		}
	case pglogrepl.XLogDataByteID:
		log.Printf("Receive XLogData message")
		xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
		if err != nil {
			return errors.Wrapf(err, "parse XLogData")
		}
		if err = c.processXLogData(&xld); err != nil {
			return errors.Wrap(err, "parse XLogData")
		}
	}

	return nil
}

func (c *pgLogicalReplicationClient) processXLogData(xld *pglogrepl.XLogData) (err error) {
	msg, err := pglogrepl.Parse(xld.WALData)
	if err != nil {
		return errors.Wrapf(err, "parse logical replication message")
	}
	c.msgBuffer = append(c.msgBuffer, msg)
	log.Printf("Receive a replication message: %s", msg.Type())
	if msg.Type() == pglogrepl.MessageTypeCommit {
		if err = c.processTransaction(); err != nil {
			return errors.Wrapf(err, "process transaction of %d messages", len(c.msgBuffer))
		}

		c.msgBuffer = c.msgBuffer[:0]
		c.walPos = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
		log.Printf("Update LSN to %s", c.walPos)
	}

	return nil
}

func (c *pgLogicalReplicationClient) processTransaction() (err error) {
	log.Printf("Process a transaction with %d messages", len(c.msgBuffer))

	// begin, op1, op2, commit
	for _, msg := range c.msgBuffer {
		switch msg := msg.(type) {
		case *pglogrepl.RelationMessage:
			c.relationMap[msg.RelationID] = msg.RelationName
			log.Printf("Relation %d %s has %d columns", msg.RelationID, msg.RelationName, msg.ColumnNum)
		case *pglogrepl.InsertMessage:
			_, err = c.getID(msg.Tuple.Columns)
			if err != nil {
				return errors.Wrap(err, "get id of a inserted column")
			}
		case *pglogrepl.UpdateMessage:
			_, err = c.getID(msg.NewTuple.Columns)
			if err != nil {
				return errors.Wrap(err, "get id of a updated column")
			}
		case *pglogrepl.DeleteMessage:
			_, err = c.getID(msg.OldTuple.Columns)
			if err != nil {
				return errors.Wrap(err, "get id of a deleted column")
			}
		}
	}

	return nil
}

func (c *pgLogicalReplicationClient) getID(columns []*pglogrepl.TupleDataColumn) (int64, error) {
	if len(columns) < 1 {
		return 0, errors.Errorf("no columns")
	}

	return columns[0].Int64()
}

func (c *pgLogicalReplicationClient) processMessages(msg pgproto3.BackendMessage) (err error) {
	switch msg := msg.(type) {
	case *pgproto3.CopyData:
		if err = c.processCopyDataMessage(msg); err != nil {
			return errors.Wrap(err, "process CopyData message")
		}
	default:
		log.Printf("Received unexpected message: %+v", msg)
	}

	return nil
}

// Run connect to Postgres and consume logical replication messages.
func (c *pgLogicalReplicationClient) Run() (err error) {
	// Create publication and replication slot.
	if err = c.prepareReplication(); err != nil {
		log.Fatalf("App %s preparing failed: %s", c.name, err)
	}

	if err = c.connect(); err != nil {
		return errors.Wrap(err, "connect to Postgres")
	}
	defer c.conn.Close(context.Background())

	if err = c.subscribeToPublication(); err != nil {
		return errors.Wrap(err, "subscribe to publication")
	}

	statusUpdateInterval := time.Second * 10
	nextDeadline := time.Now().Add(statusUpdateInterval)

	for {
		select {
		case <-c.replyRequestCh:
			nextDeadline = time.Time{}
		default:
		}

		if time.Now().After(nextDeadline) {
			log.Printf("Update standby status with LSN %s", c.walPos)
			err := pglogrepl.SendStandbyStatusUpdate(context.Background(), c.conn,
				pglogrepl.StandbyStatusUpdate{WALWritePosition: c.walPos})
			if err != nil {
				return errors.Wrapf(err, "send status update message on LSN %s", c.walPos)
			}
			nextDeadline = time.Now().Add(statusUpdateInterval)
		}

		ctx, cancel := context.WithDeadline(context.Background(), nextDeadline)
		msg, err := c.conn.ReceiveMessage(ctx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			return errors.Wrap(err, "receive message")
		}

		if err = c.processMessages(msg); err != nil {
			return errors.Wrap(err, "process message")
		}
	}
}
