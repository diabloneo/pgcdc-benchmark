package main

import (
	"log"

	"github.com/pkg/errors"

	"pgcdc-benchmark/model"
)

func migrate(db *model.DB) (err error) {
	log.Printf("Drop tables")
	if err = db.Migrator().DropTable(model.AllModels...); err != nil {
		return errors.Wrap(err, "drop tables")
	}
	log.Printf("Auto migrate all tables")
	if err = db.Migrator().AutoMigrate(model.AllModels...); err != nil {
		return errors.Wrap(err, "auto migrate")
	}

	return nil
}
