package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"

	"pgcdc-benchmark/model"
)

var (
	host     string
	port     int
	user     string
	password string
	dbname   string

	consumerNum int
)

func init() {
	flag.StringVar(&host, "host", "127.0.0.1", "Postgres server address")
	flag.IntVar(&port, "port", 5432, "Postgres server port")
	flag.StringVar(&user, "user", "postgres", "Account to login")
	flag.StringVar(&password, "password", "password", "Account password")
	flag.StringVar(&dbname, "dbname", "cdc", "Database for benchmark")
	flag.IntVar(&consumerNum, "n", 1, "Number of consumers")
}

const (
	envID = "consumer"
)

func main() {
	flag.Parse()
	log.Printf("Postgres CDC events consumers")

	var err error

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	arg := &model.ConnectionArg{
		Host:     host,
		Port:     port,
		User:     user,
		Password: password,
		DBName:   dbname,
	}
	if err = model.RegisterDB(envID, arg); err != nil {
		log.Fatalf("Failed to connect to database %s: %s", arg, err)
	}
	defer func() {
		if err := model.CloseDB(envID); err != nil {
			log.Fatalf("Failed to close db connection: %s", err)
		}
	}()
	log.Printf("Connect to database %s successfully", arg)

	clients := make([]*pgLogicalReplicationClient, 0, consumerNum)
	for i := 0; i < consumerNum; i++ {
		client := NewPgLogicalReplicationClient(fmt.Sprintf("%d", i), arg)
		clients = append(clients, client)
	}

	for _, client := range clients {
		go func(client *pgLogicalReplicationClient) {
			if err := client.Run(); err != nil {
				log.Printf("App %s failed: %s", client.name, err)
			}
		}(client)
	}

	<-c
	log.Printf("Exit by user")
}
