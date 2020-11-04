package mysql

import (
	"database/sql"
	"fmt"
	"strings"
)

// data struct for sql values or conditions
type SqlMap map[string]interface{}

type typer interface {
	Execute(string, ...[]interface{}) (sql.Result, error)
	Query(string, ...interface{}) (*sql.Rows, error)
	QueryRow(string, ...interface{}) *sql.Row
}
type mysqlx struct {
	// db connection or transaction connection
	typer
	// the table prefix
	tablePrefix
}

// Create the where SQL & Params from a Mapping data
// and Return the Where SQL & Params
func (this *mysqlx) getWhere(data SqlMap) (string, []interface{}) {
	var fields []string
	var values []interface{}
	for k, v := range data {
		if v == nil {
			fields = append(fields, k)
		} else {
			fields = append(fields, fmt.Sprintf("`%s` = ?", k))
			values = append(values, v)
		}
	}
	sql := ""
	if len(whereFields) > 0 {
		sql = strings.Join(fields, " AND ")
	}
	return sql, values
}

// Create the Insert/Update SQL & Params from a Mapping data
// and Return the Insert/Update SQL & Params
func (this *mysqlx) getValues(data SqlMap) (string, []interface{}) {
	var fields []string
	var values []interface{}
	for k, v := range data {
		if v == nil {
			fields = append(fields, fmt.Sprintf("`%s` = NULL", k))
		} else {
			switch v.(type) {
			case string:
				if strings.ToUpper(v.(string)) == "null" {
					fields = append(fields, fmt.Sprintf("`%s` = NULL", k))
				} else {
					if strings.ToUpper(v.(string)) == "now()" {
						fields = append(fields, fmt.Sprintf("`%s` = NOW()", k))
					} else {
						fields = append(fields, fmt.Sprintf("`%s` = ?", k))
						values = append(values, v)
					}
				}
			default:
				fields = append(fields, fmt.Sprintf("`%s` = ?", k))
				values = append(values, v)
			}
		}
	}
	sql := ""
	if len(fields) > 0 {
		sql = strings.Join(fields, ", ")
	}
	return sql, values
}

// Get the table name with possible table prefix
// if the prefix is empty, return the table name directly
// otherwise return the prefix_tablename
func (this *mysqlx) TableName(name string) string {
	if config.DBPrefix == "" {
		return name
	}
	return fmt.Sprintf("%s_%s", config.DBPrefix, name)
}
