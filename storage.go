package dspider

import (
	"bytes"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/golang/glog"
)

const (
	SQL_STRUCT_TAG_NAME  = "sql"
	SQL_STRUCT_TAG_TABLE = "table"
)

type Storage interface {
	AddDoc(doc interface{}) error
}

type SqlStorage struct {
	mu sync.Mutex
	db *sql.DB
}

type SqlTableDef struct {
	Name        string
	Columns     map[string]string
	PrimaryKeys []string
}

func NewSqlStorage(driver, fileName string, tableDefs []SqlTableDef) (*SqlStorage, error) {
	db, err := sql.Open(driver, fileName)
	if err != nil {
		return nil, err
	}
	for _, def := range tableDefs {
		if err := createTable(db, def); err != nil {
			return nil, err
		}
	}
	return &SqlStorage{db: db}, nil
}

func (s *SqlStorage) Close() error {
	return s.db.Close()
}

func (s *SqlStorage) AddDoc(doc interface{}) error {
	t := reflect.TypeOf(doc)
	if t.Kind() != reflect.Ptr {
		glog.Fatalf("Expecting a struct pointer, got %v", t)
	}
	t = t.Elem()
	v := reflect.ValueOf(doc).Elem()
	var table string
	columnMap := make(map[string]interface{})
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		fv := v.Field(i)
		if column := sf.Tag.Get(SQL_STRUCT_TAG_NAME); column == SQL_STRUCT_TAG_TABLE {
			if sf.Type.Kind() != reflect.String {
				glog.Fatalf("The field with 'table' tag should be string, got: %v", sf.Type)
			}
			table = fv.String()
		} else if column == "" {
			glog.Warningf("Field '%s.%s' doesn't have tag '%s'.", sf.Type.Name(), sf.Name,
				SQL_STRUCT_TAG_NAME)
		} else {
			columnMap[column] = fv.Interface()
		}
	}
	return s.insert(table, columnMap)
}

func (s *SqlStorage) insert(table string, columnMap map[string]interface{}) error {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "INSERT INTO %s (", table)
	var args []interface{}
	first := true
	for name, value := range columnMap {
		if first {
			first = false
		} else {
			buf.WriteString(", ")
		}
		buf.WriteString(name)
		args = append(args, value)
	}
	buf.WriteString(") VALUES (")
	buf.WriteString(strings.Repeat("?, ", len(args)-1))
	buf.WriteString("?)")
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(buf.String(), args...)
	return err
}

func createTable(db *sql.DB, def SqlTableDef) error {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "CREATE TABLE IF NOT EXISTS %s (", def.Name)
	first := true
	for col, body := range def.Columns {
		if first {
			first = false
		} else {
			buf.WriteString(",")
		}
		fmt.Fprintf(&buf, "\n    %s %s", col, body)
	}
	if len(def.PrimaryKeys) > 0 {
		fmt.Fprintf(&buf, ",\n   PRIMARY KEY(%s)", strings.Join(def.PrimaryKeys, ", "))
	}
	buf.WriteString("\n)")
	if _, err := db.Exec(buf.String()); err != nil {
		return fmt.Errorf("failed to create table '%s': %v", def.Name, err)
	}
	return nil
}
