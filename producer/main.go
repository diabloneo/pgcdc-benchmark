package main

import (
	"flag"
	"log"

	"pgcdc-benchmark/model"
)

var (
	host     string
	port     int
	user     string
	password string
	dbname   string
)

func init() {
	flag.StringVar(&host, "host", "127.0.0.1", "Postgres server address")
	flag.IntVar(&port, "port", 5432, "Postgres server port")
	flag.StringVar(&user, "user", "postgres", "Account to login")
	flag.StringVar(&password, "password", "password", "Account password")
	flag.StringVar(&dbname, "dbname", "cdc", "Database for benchmark")
}

const (
	envID = "producer"
)

func main() {
	flag.Parse()
	log.Printf("Postgres CDC events producer")

	var err error

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

	db := model.NewDB(envID)
	if err = migrate(db); err != nil {
		log.Fatalf("Failed to migrate database: %s", err)
	}

	if err = generateBook("book"); err != nil {
		log.Fatalf("Failed to create book: %s", err)
	}
}
