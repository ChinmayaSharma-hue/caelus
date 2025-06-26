package db

import "gorm.io/gorm"

type FilterOperator string

const (
	EQ        FilterOperator = "="
	NEQ       FilterOperator = "!="
	GT        FilterOperator = ">"
	LT        FilterOperator = "<"
	GTE       FilterOperator = ">="
	LTE       FilterOperator = "<="
	LIKE      FilterOperator = "LIKE"
	ILIKE     FilterOperator = "ILIKE"
	IN        FilterOperator = "IN"
	NOTIN     FilterOperator = "NOT IN"
	BETWEEN   FilterOperator = "BETWEEN"
	ISNULL    FilterOperator = "IS NULL"
	ISNOTNULL FilterOperator = "IS NOT NULL"
	ASC       FilterOperator = "ASC"
	SIMILARTO FilterOperator = "SIMILAR TO"
)

func (fo FilterOperator) String() string {
	return string(fo)
}

type TransactionOperation struct {
	TxID string
	db   *gorm.DB
}
