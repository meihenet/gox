package db

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// the normal class for connect to mysql server
type Mysql struct {
	mysqlx
	conn *sql.DB
	// the max connection life time
	connMaxLiftTime time.Duration
	// the max idle connections
	maxIdleConns int
}

type mysqlOptions func(*Mysql)

// Get a mysql client object
var client *Mysql
var once sync.Once

func NewMysql(dsn string, opts ...mysqlOptions) (*Mysql, error) {
	var err error = nil
	once.Do(func() {
		client = &Mysql{}
		client.sqlTyper = client
		client.prefix = ""
		client.connMaxLiftTime = 100
		client.maxIdleConns = 10
		for _, opt := range opts {
			opt(client)
		}
		err = client.connect(dsn)
	})
	return client, err
}

func WithMysqlPrefix(prefix string) mysqlOptions {
	return func(mysql *Mysql) {
		mysql.prefix = prefix
	}
}

func WithMysqlConnMaxLiftTime(connMaxLiftTime time.Duration) mysqlOptions {
	return func(mysql *Mysql) {
		mysql.connMaxLiftTime = connMaxLiftTime
	}
}

func WithMysqlMaxIdleConns(maxIdleConns int) mysqlOptions {
	return func(mysql *Mysql) {
		mysql.maxIdleConns = maxIdleConns
	}
}

// try to connect to the DB host
// @DNS:  <user>:<pass>@tcp(<host>:<port>)/<dbName>?charset=<charset>
func (this *Mysql) connect(dsn string) error {
	conn, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
		return err
	}
	// Set the max connections
	conn.SetConnMaxLifetime(this.connMaxLiftTime)
	// Set the max idle connections
	conn.SetMaxIdleConns(this.maxIdleConns)
	// Double check the db connection works
	if err := conn.Ping(); err != nil {
		log.Fatal(err)
		return err
	}
	this.conn = conn
	return nil
}

func (this *Mysql) Execute(sql string, params ...[]interface{}) (sql.Result, error) {
	stmt, err := this.conn.Prepare(sql)
	if err != nil {
		return nil, err
	}

	var args []interface{}
	if len(params) > 0 {
		args = params[0]
	}

	res, err := stmt.Exec(args...)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (this *Mysql) Query(sql string, args ...interface{}) (*sql.Rows, error) {
	return this.conn.Query(sql, args...)
}

func (this *Mysql) QueryRow(sql string, args ...interface{}) *sql.Row {
	return this.conn.QueryRow(sql, args...)
}

// try to close the db connection and set to nil
func (this *Mysql) Close() {
	if this.conn != nil {
		this.conn.Close()
		this.conn = nil
	}
}

// Create a new transaction object
func (this *Mysql) StartTx() (*MysqlTx, error) {
	tx, err := this.conn.Begin()
	if err != nil {
		return nil, err
	}

	mysqlTx := &MysqlTx{conn: tx}
	mysqlTx.sqlTyper = mysqlTx
	mysqlTx.mysqlx.prefix = this.mysqlx.prefix
	return mysqlTx, nil
}

////////////////////////////////////////////////////////////////////////////
/// class mysqlx
////////////////////////////////////////////////////////////////////////////
type mysqlx struct {
	// db connection or transaction connection
	sqlTyper
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
