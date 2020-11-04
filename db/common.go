package db

import (
	"database/sql"
)

// data struct for sql values or conditions
type SqlMap map[string]interface{}

type typer interface {
	Execute(string, ...[]interface{}) (sql.Result, error)
	Query(string, ...interface{}) (*sql.Rows, error)
	QueryRow(string, ...interface{}) *sql.Row
}

// Self-add or Self-minus one field
type sqlAddOrMinusOption struct {
	wheres SqlMap
	step   int
}

func WithAddOrMinusWheres(wheres SqlMap) func(*sqlAddOrMinusOption) {
	return func(opt *sqlAddOrMinusOption) {
		opt.wheres = wheres
	}
}

func WithAddOrMinusStep(step int) func(*sqlAddOrMinusOption) {
	return func(opt *sqlAddOrMinusOption) {
		opt.step = step
	}
}
