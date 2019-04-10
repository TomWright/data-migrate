package data_migrate

import (
	"database/sql"
	"fmt"
)

// ProcessRowFunc is used to process each row exported from the source table.
type ProcessRowFunc func(row *Row) error

// NewMigrate returns a new MigrateContext
func NewMigrate(from *TableContext, paginationLimit int) *MigrateContext {
	return &MigrateContext{
		From:            from,
		RowProcessors:   make([]ProcessRowFunc, 0),
		Defers:          make([]func(), 0),
		PaginationLimit: paginationLimit,
	}
}

// MigrateContext
type MigrateContext struct {
	From            *TableContext
	RowProcessors   []ProcessRowFunc
	Defers          []func()
	PaginationLimit int
	SelectStmt      *sql.Stmt
}

func (x *MigrateContext) WithProcessor(processors ...ProcessRowFunc) *MigrateContext {
	x.RowProcessors = append(x.RowProcessors, processors...)
	return x
}

func (x *MigrateContext) WithDefer(defers ...func()) *MigrateContext {
	x.Defers = append(x.Defers, defers...)
	return x
}

func migratePage(c *MigrateContext, offset int) (bool, error) {
	var err error
	if c.SelectStmt == nil {
		c.SelectStmt, err = c.From.DB.Prepare(fmt.Sprintf("SELECT * FROM `%s`.`%s` LIMIT ? OFFSET ?", c.From.DBName, c.From.Table))
		if err != nil {
			return false, err
		}
	}

	rows, err := c.SelectStmt.Query(c.PaginationLimit, offset)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return false, err
	}
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return false, err
	}
	columnIdentifiers := make(map[string]int)
	for k, v := range columns {
		columnIdentifiers[v] = k
	}

	ok := false

rowLoop:
	for rows.Next() {
		ok = true
		row := &Row{
			Columns: make([]*Column, len(columns)),
		}
		scanData := make([]interface{}, len(columns))
		for k := range row.Columns {
			row.Columns[k] = &Column{
				Column: columns[k],
				Type:   columnTypes[k],
			}
			scanData[k] = &row.Columns[k].Value
		}

		if err := rows.Scan(scanData...); err != nil {
			return false, err
		}

		if len(c.RowProcessors) > 0 {
			for _, p := range c.RowProcessors {
				err = p(row)
				if err == ErrSkipRow {
					continue rowLoop
				}
				if err != nil {
					return false, err
				}
			}
		}
	}

	return ok, nil
}

// Migrate kicks off the process of copying from the source table to the destination table.
func Migrate(c *MigrateContext) error {
	defer func(c *MigrateContext) {
		for _, d := range c.Defers {
			d()
		}
		if c.SelectStmt != nil {
			c.SelectStmt.Close()
			c.SelectStmt = nil
		}
	}(c)

	offset := 0

paginationLoop:
	for {
		ok, err := migratePage(c, offset)
		if err != nil {
			return err
		}
		if !ok {
			break paginationLoop
		}
		offset += c.PaginationLimit
	}

	return nil
}
