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
	prefix string
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
	if len(fields) > 0 {
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
	if this.prefix == "" {
		return name
	}
	return fmt.Sprintf("%s_%s", this.prefix, name)
}

// Query multiple rows
func (this *mysqlx) FetchRows(sql string, wheres ...SqlMap) (*sql.Rows, error) {
	var args []interface{}
	if len(wheres) == 1 {
		wheres := wheres[0]
		if wheres != nil {
			whereSql, params := this.getWhere(wheres)
			if whereSql != "" {
				sql += fmt.Sprintf(" WHERE %s", whereSql)
				args = append(args, params...)
			}
		}
	}
	return this.Query(sql, args...)
}

// Query a single row
func (this *mysqlx) FetchRow(sql string, wheres ...SqlMap) *sql.Row {
	var args []interface{}
	if len(wheres) == 1 {
		wheres := wheres[0]
		if wheres != nil {
			whereSql, params := this.getWhere(wheres)
			if whereSql != "" {
				sql += fmt.Sprintf(" WHERE %s", whereSql)
				args = append(args, params...)
			}
		}
	}
	return this.QueryRow(sql, args...)
}

// Get the records count by some conditions
func (this *mysqlx) Count(tableName string, wheres ...SqlMap) int64 {
	var numRows int64 = 0
	sql := fmt.Sprintf("SELECT COUNT(0) AS `num_rows` FROM `%s`", this.TableName(tableName))
	row := this.FetchRow(sql, wheres...)
	row.Scan(&numRows)
	return numRows
}

// Insert data into table
func (this *mysqlx) Insert(tableName string, data SqlMap) (int64, error) {
	fieldsSql, values := this.getValues(data)
	sql := fmt.Sprintf("INSERT INTO `%s` SET %s", this.TableName(tableName), fieldsSql)

	res, err := this.Execute(sql, values)
	if err != nil {
		return 0, err
	}

	lastId, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}

	return lastId, nil
}

// Update data to table
func (this *mysqlx) Update(tableName string, data SqlMap, wheres ...SqlMap) (int64, error) {
	fieldsSql, values := this.getValues(data)
	sql := fmt.Sprintf("UPDATE `%s` SET %s", this.TableName(tableName), fieldsSql)
	args := values
	if len(wheres) > 0 {
		wheres := wheres[0]
		if wheres != nil {
			whereSql, params := this.getWhere(wheres)
			if whereSql != "" {
				sql += fmt.Sprintf(" WHERE %s", whereSql)
				args = append(args, params...)
			}
		}
	}

	res, err := this.Execute(sql, args)
	if err != nil {
		return 0, err
	}

	affectRows, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}

	return affectRows, nil
}

// Delete data from table
func (this *mysqlx) Delete(tableName string, wheres ...SqlMap) (int64, error) {
	sql := fmt.Sprintf("DELETE FROM `%s`", this.TableName(tableName))
	var args []interface{}
	if len(wheres) > 0 {
		wheres := wheres[0]
		whereSql, whereValues := this.getWhere(wheres)
		args = whereValues
		if whereSql != "" {
			sql += fmt.Sprintf(" WHERE %s", whereSql)
		}
	}

	res, err := this.Execute(sql, args)
	if err != nil {
		return 0, err
	}

	affectRows, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return affectRows, nil
}

// Self-add or Self-minus one field
type sqlAddOrMinusOption struct {
	wheres SqlMap
	step   int
}

func (this *mysqlx) WithAddOrMinusWheres(wheres SqlMap) func(*sqlAddOrMinusOption) {
	return func(opt *sqlAddOrMinusOption) {
		opt.wheres = wheres
	}
}

func (this *mysqlx) WithAddOrMinusStep(step int) func(*sqlAddOrMinusOption) {
	return func(opt *sqlAddOrMinusOption) {
		opt.step = step
	}
}

func (this *mysqlx) Add(tableName string, column string, funcs ...func(*sqlAddOrMinusOption)) (int64, error) {
	opt := &sqlAddOrMinusOption{wheres: nil, step: 1}

	for _, f := range funcs {
		f(opt)
	}

	var args []interface{}
	sql := fmt.Sprintf("UPDATE `%s` SET `%s` = `%s` + ?", this.TableName(tableName), column, column)
	args = append(args, opt.step)
	if opt.wheres != nil {
		whereSql, params := this.getWhere(opt.wheres)
		if whereSql != "" {
			sql += fmt.Sprintf(" WHERE %s", whereSql)
			args = append(args, params...)
		}
	}

	res, err := this.Execute(sql, args)
	if err != nil {
		return 0, err
	}

	affectRows, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return affectRows, nil
}

func (this *mysqlx) Minus(tableName string, column string, funcs ...func(*sqlAddOrMinusOption)) (int64, error) {
	opt := &sqlAddOrMinusOption{wheres: nil, step: 1}

	for _, f := range funcs {
		f(opt)
	}

	var args []interface{}
	sql := fmt.Sprintf("UPDATE `%s` SET `%s` = `%s` - ?", this.TableName(tableName), column, column)
	args = append(args, opt.step)
	if opt.wheres != nil {
		whereSql, params := this.getWhere(opt.wheres)
		if whereSql != "" {
			sql += fmt.Sprintf(" WHERE %s", whereSql)
			args = append(args, params...)
		}
	}

	res, err := this.Execute(sql, args)
	if err != nil {
		return 0, err
	}

	affectRows, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return affectRows, nil
}
