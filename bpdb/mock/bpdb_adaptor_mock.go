package mock

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
	return []schema.Event{
		schema.MakeNewEvent("event0", 0),
		schema.MakeNewEvent("event0", 1),
		schema.MakeNewEvent("event0", 2),
		schema.MakeNewEvent("event1", 0),
		schema.MakeNewEvent("event1", 1),
		schema.MakeNewEvent("event1", 2),
		schema.MakeNewEvent("event2", 0),
		schema.MakeNewEvent("event2", 1),
		schema.MakeNewEvent("event2", 2),
	}, nil
}

func (mpg *MockPostgresBackendObject) GetNewestEvent(eventName string) (schema.Event, error) {
	return schema.MakeNewEvent(eventName, 10), nil
}

func (mpg *MockPostgresBackendObject) GetSpecificEvent(eventName string, eventVersion int) (schema.Event, error) {
	return schema.MakeNewEvent(eventName, eventVersion), nil
}

func (mpg *MockPostgresBackendObject) UpdateEvent(event schema.Event) error {
	return nil
}
