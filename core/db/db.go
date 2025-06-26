package db

import (
	"fmt"
	"github.com/ChinmayaSharma-hue/caelus/core/config"
	"github.com/google/uuid"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"reflect"
)

type FiltersMap map[FilterOperator]map[string]interface{}

type Database interface {
	GetTransactionObject(transactionObject *TransactionOperation) *gorm.DB
	BeginTransaction() *TransactionOperation
	RollbackTransaction(transactionObject *TransactionOperation) error
	CommitTransaction(transactionObject *TransactionOperation) error
	Insert(entitySlice interface{}, transactionObject *TransactionOperation) error
	FilterBy(pageToken string, filters FiltersMap, transactionObject *TransactionOperation, entitySlice interface{}) error
}

type pgDatabase struct {
	db *gorm.DB
}

func NewPgDatabase(cfg config.PostgresConfig) (Database, error) {
	dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%d sslmode=disable",
		cfg.Host,
		cfg.Username,
		cfg.Password,
		cfg.Name,
		cfg.Port,
	)

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, err
	}

	return &pgDatabase{db: db}, nil
}

func (d *pgDatabase) GetTransactionObject(transactionObject *TransactionOperation) *gorm.DB {
	if transactionObject == nil || transactionObject.TxID == "" || transactionObject.db == nil {
		return d.db
	}
	return transactionObject.db
}

func (d *pgDatabase) BeginTransaction() *TransactionOperation {
	return &TransactionOperation{TxID: uuid.New().String(), db: d.db.Begin()}
}

func (d *pgDatabase) RollbackTransaction(transactionObject *TransactionOperation) error {
	if transactionObject == nil || transactionObject.TxID == "" || transactionObject.db == nil {
		return fmt.Errorf("transaction object is nil")
	}
	return transactionObject.db.Rollback().Error
}

func (d *pgDatabase) CommitTransaction(transactionObject *TransactionOperation) error {
	if transactionObject == nil || transactionObject.TxID == "" || transactionObject.db == nil {
		return fmt.Errorf("transaction object is nil")
	}
	return transactionObject.db.Commit().Error
}

func (d *pgDatabase) Insert(entity interface{}, transactionObject *TransactionOperation) error {
	tx := d.GetTransactionObject(transactionObject)

	// Check if record with the same ID exists
	var count int64
	if err := tx.Model(entity).Where("id = ?", getID(entity)).Count(&count).Error; err != nil {
		return err
	}

	if count > 0 {
		// Entry with same ID already exists
		return nil
	}

	return tx.Create(entity).Error
}

func (d *pgDatabase) FilterBy(pageToken string, filters FiltersMap, transactionObject *TransactionOperation, entitySlice interface{}) error {
	query := d.GetTransactionObject(transactionObject).Model(entitySlice).Limit(MaxResultsDefault)
	for operator, keyValMap := range filters {
		for key, val := range keyValMap {
			query = query.Where(fmt.Sprintf("%s %s ?", key, operator), val)
		}
	}
	if pageToken != "" {
		query = query.Where("id > ?", pageToken)
	}
	err := query.Find(entitySlice).Error
	if err != nil {
		return err
	}
	return nil
}

func getID(entity interface{}) interface{} {
	val := reflect.ValueOf(entity)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	idField := val.FieldByName("ID")
	if !idField.IsValid() {
		return nil
	}
	return idField.Interface()
}
