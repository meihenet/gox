package mysql

import (
	"database/sql"
	"log"
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

func New(dsn string, opts ...mysqlOptions) (*Mysql, error) {
	var err error = nil
	once.Do(func() {
		client = &Mysql{}
		client.typer = client
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

func WithPrefix(prefix string) mysqlOptions {
	return func(mysql *Mysql) {
		mysql.prefix = prefix
	}
}

func WithConnMaxLiftTime(connMaxLiftTime time.Duration) mysqlOptions {
	return func(mysql *Mysql) {
		mysql.connMaxLiftTime = connMaxLiftTime
	}
}

func WithMaxIdleConns(maxIdleConns int) mysqlOptions {
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
	mysqlTx.typer = mysqlTx
	mysqlTx.mysqlx.prefix = this.mysqlx.prefix
	return mysqlTx, nil
}
