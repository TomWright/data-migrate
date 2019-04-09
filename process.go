package data_migrate

import (
	"database/sql"
	"fmt"
	"strings"
)

func InsertRowFunc(insertStmt *sql.Stmt, to *TableContext) ProcessRowFunc {
	return func(row *Row) error {
		var err error

		if insertStmt == nil {
			insertColumns := make([]string, len(row.Columns))
			for k, c := range row.Columns {
				insertColumns[k] = c.Column
			}
			insertStmt, err = to.DB.Prepare(fmt.Sprintf(
				"INSERT INTO %s.%s (%s) VALUES(%s)",
				to.DBName,
				to.Table,
				strings.Join(insertColumns, ", "),
				strings.TrimSuffix(strings.Repeat("?, ", len(insertColumns)), ", "),
			))
			if err != nil {
				return err
			}
		}

		binds := make([]interface{}, len(row.Columns))
		for k, v := range row.Columns {
			binds[k] = v.Value
		}

		_, err = insertStmt.Exec(binds...)
		if err != nil {
			return err
		}

		return nil
	}
}

func UpsertRowFunc(insertStmt *sql.Stmt, to *TableContext) ProcessRowFunc {
	return func(row *Row) error {
		var err error
		if insertStmt == nil {
			insertColumns := make([]string, len(row.Columns))
			upsertColumns := make([]string, len(row.Columns))
			for k, c := range row.Columns {
				insertColumns[k] = c.Column
				upsertColumns[k] = c.Column + " = ?"
			}
			insertStmt, err = to.DB.Prepare(fmt.Sprintf(
				"INSERT INTO %s.%s (%s) VALUES(%s) ON DUPLICATE KEY UPDATE %s",
				to.DBName,
				to.Table,
				strings.Join(insertColumns, ", "),
				strings.TrimSuffix(strings.Repeat("?, ", len(insertColumns)), ", "),
				strings.Join(upsertColumns, ", "),
			))
			if err != nil {
				return err
			}
		}

		binds := make([]interface{}, len(row.Columns)*2)
		// insert binds
		for k, v := range row.Columns {
			binds[k] = v.Value
		}
		// upsert binds
		for k, v := range row.Columns {
			binds[k] = v.Value
		}

		_, err = insertStmt.Exec(binds...)
		if err != nil {
			return err
		}

		return nil
	}
}
