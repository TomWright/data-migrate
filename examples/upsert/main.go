package main

import (
	"database/sql"
	"flag"
	"fmt"
	"github.com/tomwright/data-migrate"
	"log"
	"os"
	"strings"
	"sync"
)

func createRowCountProcessor(table string, rowsProcessedMu *sync.Mutex, rowsProcessed map[string]int64) func(row *data_migrate.Row) error {
	return func(row *data_migrate.Row) error {
		rowsProcessedMu.Lock()
		defer rowsProcessedMu.Unlock()
		if _, ok := rowsProcessed[table]; !ok {
			rowsProcessed[table] = 0
		}
		rowsProcessed[table]++
		return nil
	}
}

func main() {
	var fromDBName, toDBName string

	fromDSN := flag.String("from", "", "The DSN for the source database")
	toDSN := flag.String("to", "", "The DSN for the destination database")
	tableFlag := flag.String("table", "", "Comma separated list of tables to migrate")
	flag.Parse()

	if fromDSN == nil || *fromDSN == "" {
		log.Println("Missing flag `from`")
		os.Exit(1)
	}
	if toDSN == nil || *toDSN == "" {
		log.Println("Missing flag `to`")
		os.Exit(1)
	}
	if tableFlag == nil || *tableFlag == "" {
		log.Println("Missing flag `table`")
		os.Exit(1)
	}
	splitTables := strings.Split(*tableFlag, ",")

	tables := make([][]string, 0)
	for _, t := range splitTables {
		tables = append(tables, strings.Split(t, ":"))
	}

	fromDB, err := sql.Open("mysql", *fromDSN)
	if err != nil {
		log.Printf("Could not connect to source database: %s\n", err)
		os.Exit(1)
	}

	toDB, err := sql.Open("mysql", *toDSN)
	if err != nil {
		log.Printf("Could not connect to destination database: %s\n", err)
		os.Exit(1)
	}

	rowsProcessedMu := &sync.Mutex{}
	rowsProcessed := make(map[string]int64)

	for _, table := range tables {
		// create a table context that contains all required information about the source table
		fromTable := data_migrate.NewTable(fromDB, fromDBName, table[0])
		// create a table context that contains all required information about the destination table
		toTable := data_migrate.NewTable(toDB, toDBName, table[1])

		// create the stmt which will be used for all upserts in this table
		var upsertStmt *sql.Stmt

		// get the upsert row processor. this processor uses the upsertStmt created above.
		upsertProcessor := data_migrate.UpsertRowFunc(upsertStmt, toTable)

		// create a migration context which defines how we want the migration to run
		migrationCtx := data_migrate.NewMigrate(fromTable, 50)

		// ensure that once this migration finishes running, we close the upsertStmt used above
		migrationCtx.WithDefer(func() {
			if upsertStmt != nil {
				_ = upsertStmt.Close()
			}
		})

		migrateTableDesc := fmt.Sprintf("`%s` -> `%s`", table[0], table[1])

		// add some row processors to the migration context...
		// 1. the processor that does the actual upsert functionality
		// 2. a processor to keep track of the rows processed for each table
		migrationCtx.WithProcessor(upsertProcessor, createRowCountProcessor(migrateTableDesc, rowsProcessedMu, rowsProcessed))

		// run the actual data migration for the table
		// the Migrate call is blocking and will wait here until all data for table has been processed
		log.Printf("Migrating %s\n", migrateTableDesc)
		if err := data_migrate.Migrate(migrationCtx); err != nil {
			log.Printf("Unexpected error when migrating %s: %s\n", migrateTableDesc, err)
			os.Exit(1)
		}
	}

	// all tables will be migrated by this point.
	// simply log out some information on how many rows were migrated for each table.
	processedLog := "Migration complete:\n"
	processedLogVars := make([]interface{}, 0)
	for k, v := range rowsProcessed {
		processedLog += "\t%s - %d rows\n"
		processedLogVars = append(processedLogVars, k, v)
	}
	log.Printf(processedLog, processedLogVars...)
}
