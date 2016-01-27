package bpdb

import (
	"database/sql"

	"github.com/twitchscience/scoop_protocol/schema"
)

type MockPostgresBackendObject struct {
	connection *sql.DB
	tableName  string
}

func BuildMockPostgresBackend(db *sql.DB, tableName string) (MockPostgresBackendObject, error) {

	return MockPostgresBackendObject{
		connection: db,
		tableName:  tableName,
	}, nil
}

func (mpg *MockPostgresBackendObject) GetEvents() ([]schema.Event, error) {
	return nil, nil
}

func (mpg *MockPostgresBackendObject) GetNewestEvent(eventName string) (schema.Event, error) {
	var event schema.Event

	return event, nil
}

func (mpg *MockPostgresBackendObject) GetSpecificEvent(eventName string, eventVersion int) (schema.Event, error) {
	var event schema.Event

	return event, nil
}

func (mpg *MockPostgresBackendObject) UpdateEvent(event schema.Event) error {

	return nil
}
