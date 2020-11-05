package db

import "database/sql"

////////////////////////////////////////////////////////////////////////////
/// class mysql tx
////////////////////////////////////////////////////////////////////////////
type MysqlTx struct {
	mysqlx
	conn *sql.Tx
}

func (this *MysqlTx) Execute(sql string, params ...[]interface{}) (sql.Result, error) {
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

func (this *MysqlTx) Query(sql string, args ...interface{}) (*sql.Rows, error) {
	return this.conn.Query(sql, args...)
}

func (this *MysqlTx) QueryRow(sql string, args ...interface{}) *sql.Row {
	return this.conn.QueryRow(sql, args...)
}

func (this *MysqlTx) Commit() error {
	return this.conn.Commit()
}

func (this *MysqlTx) Rollback() error {
	return this.conn.Rollback()
}
