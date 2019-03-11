package data_migrate

import "database/sql"

// Column contains the value of a column, as-well as the type of column it was retrieved from
type Column struct {
	Column string
	Value  interface{}
	Type   *sql.ColumnType
}

type Row struct {
	// Columns contains the set of columns in the order they were selected/scanned
	Columns []*Column
	// ColumnMap is a map where the column name maps to the associated column
	ColumnMap map[string]*Column
}
