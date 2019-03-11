package data_migrate

import "database/sql"

// NewTable returns a new TableContext
func NewTable(db *sql.DB, dbName string, table string) *TableContext {
	return &TableContext{
		DB:     db,
		DBName: dbName,
		Table:  table,
	}
}

// TableContext represents a table within a database, with a connection to this database.
type TableContext struct {
	DB     *sql.DB
	DBName string
	Table  string
}
