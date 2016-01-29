package bpdb

import (
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"reflect"
	"testing"

	"github.com/twitchscience/scoop_protocol/schema"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/lib/pq"
)

var (
	postgresURL        = flag.String("postgresURL", "", "The login url for the postgres DB")
	postgresMockStatus = flag.Bool("pgMockStatus", true, "Whether you want to use the mock db for testing or not.  Defaults to true")
	testEventTable     = "test_event_schemas"
	pgBackend          PostgresBackendObject
)

func setupTestDB() (PostgresBackendObject, error) {

	db, err := sql.Open("postgres", *postgresURL)
	if err != nil {
		return PostgresBackendObject{}, fmt.Errorf("Could not extablish connection to DB: %v", err)
	}

	pgBackend, err := NewPostgresBackend(db, testEventTable)
	if err != nil {
		return PostgresBackendObject{}, fmt.Errorf("Could not extablish connection to DB and store to postgres object: %v", err)
	}

	testEvents := []schema.Event{
		schema.MakeNewEvent("event0", 0),
		schema.MakeNewEvent("event0", 1),
		schema.MakeNewEvent("event0", 2),
		schema.MakeNewEvent("event1", 0),
		schema.MakeNewEvent("event1", 1),
		schema.MakeNewEvent("event1", 2),
		schema.MakeNewEvent("event2", 0),
		schema.MakeNewEvent("event2", 1),
		schema.MakeNewEvent("event2", 2),
	}

	pgBackend.createTestTable()
	for _, testEvent := range testEvents {
		err := pgBackend.PutEvent(testEvent)
		if err != nil {
			return PostgresBackendObject{}, fmt.Errorf("Could not add test events to DB: %v", err)
		}
	}

	return pgBackend, nil
}

func DeepEqualChecker(expectedTestEvent, actualTestEvent schema.Event, t *testing.T) {
	if !reflect.DeepEqual(actualTestEvent, expectedTestEvent) {
		t.Errorf("Event grabbed from db was incorrect")
		t.Logf("Expected response: %+v", expectedTestEvent)
		t.Logf("Actual response: %+v", actualTestEvent)
	}
}

func TestEvents(t *testing.T) {
	flag.Parse()

	if *postgresMockStatus {
		t.SkipNow()
	}

	pgBackend, err := setupTestDB()
	if err != nil {
		t.Fatalf("Error %v setting up DB for test", err)
	}
	defer pgBackend.connection.Close()
	defer pgBackend.dropTestTable()

	newestTestEvents, err := pgBackend.Events()
	if err != nil {
		t.Fatalf("Could not get events from db: %s", err)
	}

	expectedTestEvents := []schema.Event{
		schema.MakeNewEvent("event0", 2),
		schema.MakeNewEvent("event1", 2),
		schema.MakeNewEvent("event2", 2),
	}

	//cannot use deepequals checker due to list arguments
	if !reflect.DeepEqual(newestTestEvents, expectedTestEvents) {
		t.Errorf("Events grabbed from db were incorrect")
		t.Logf("Expected response: %+v", expectedTestEvents)
		t.Logf("Actual response: %+v", newestTestEvents)
	}
}

func TestNewestEvent(t *testing.T) {
	flag.Parse()

	if *postgresMockStatus {
		t.SkipNow()
	}

	pgBackend, err := setupTestDB()
	if err != nil {
		t.Fatalf("Error %v setting up DB for test", err)
	}
	defer pgBackend.connection.Close()
	defer pgBackend.dropTestTable()

	newestTestEvent, err := pgBackend.NewestEvent("event2")
	if err != nil {
		t.Fatalf("Could not get event from db: %s", err)
	}

	expectedTestEvent := schema.MakeNewEvent("event2", 2)

	DeepEqualChecker(expectedTestEvent, newestTestEvent, t)

	newestTestEvent, err = pgBackend.NewestEvent("event3")
	if err != nil {
		t.Fatalf("Could not get event from db: %s", err)
	}

	expectedTestEvent = schema.MakeNewEvent("event3", 1)

	DeepEqualChecker(expectedTestEvent, newestTestEvent, t)
}

func TestEventVersionGeneric(t *testing.T) {
	flag.Parse()

	if *postgresMockStatus {
		t.SkipNow()
	}

	pgBackend, err := setupTestDB()
	if err != nil {
		t.Fatalf("Error %v setting up DB for test", err)
	}
	defer pgBackend.connection.Close()
	defer pgBackend.dropTestTable()

	specificTestEvent, err := pgBackend.EventVersion("event2", 1)
	if err != nil {
		t.Fatalf("Could not get event from db: %s", err)
	}

	expectedTestEvent := schema.MakeNewEvent("event2", 1)

	DeepEqualChecker(expectedTestEvent, specificTestEvent, t)
}

func TestEventVersionExistance(t *testing.T) {
	flag.Parse()

	if *postgresMockStatus {
		t.SkipNow()
	}

	pgBackend, err := setupTestDB()
	if err != nil {
		t.Fatalf("Error %v setting up DB for test", err)
	}
	defer pgBackend.connection.Close()
	defer pgBackend.dropTestTable()

	specificTestEvent, err := pgBackend.EventVersion("event3", 1)
	if err == nil {
		t.Errorf("Should have errored due to event not existing but did not")
		t.Logf("Unexpected response: %+v", specificTestEvent)
	}
}

func TestEventVersionVersion(t *testing.T) {
	flag.Parse()

	if *postgresMockStatus {
		t.SkipNow()
	}

	pgBackend, err := setupTestDB()
	if err != nil {
		t.Fatalf("Error %v setting up DB for test", err)
	}
	defer pgBackend.connection.Close()
	defer pgBackend.dropTestTable()

	specificTestEvent, err := pgBackend.EventVersion("event2", 3)
	if err == nil {
		t.Errorf("Should have errored due to event version not existing but did not")
		t.Logf("Unexpected response: %+v", specificTestEvent)
	}

}

func eventStringGenerator(event schema.Event, t *testing.T) string {
	jsonEvent, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("Error %v Marshalling json for test event creation", err)
	}
	return string(jsonEvent)
}

func TestMockEvents(t *testing.T) {
	flag.Parse()

	db, err := sqlmock.New()

	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	mockBackend, err := NewPostgresBackend(db, testEventTable)
	if err != nil {
		t.Fatalf("an error '%s' was not expected when creating backend object and tested with ping", err)
	}

	defer db.Close()

	tableSchema := []string{"event_name", "event_version", "event_payload"}
	mockRows := sqlmock.NewRows(tableSchema).
		AddRow("event0", 2, eventStringGenerator(schema.MakeNewEvent("event0", 2), t)).
		AddRow("event1", 2, eventStringGenerator(schema.MakeNewEvent("event1", 2), t)).
		AddRow("event2", 2, eventStringGenerator(schema.MakeNewEvent("event2", 2), t))

	sqlmock.ExpectQuery("select").WillReturnRows(mockRows)

	allMockEvents, err := mockBackend.Events()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when getting all newest events", err)
	}

	expectedEvents := []schema.Event{schema.MakeNewEvent("event0", 2), schema.MakeNewEvent("event1", 2), schema.MakeNewEvent("event2", 2)}

	if !reflect.DeepEqual(allMockEvents, expectedEvents) {
		t.Errorf("Incorrect Row Response")
		fmt.Println(allMockEvents)
		fmt.Println(expectedEvents)
	}

}

func TestMockNewestEvent(t *testing.T) {
	flag.Parse()

	db, err := sqlmock.New()

	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	mockBackend, err := NewPostgresBackend(db, testEventTable)
	if err != nil {
		t.Fatalf("an error '%s' was not expected when creating backend object and tested with ping", err)
	}

	defer db.Close()

	tableSchema := []string{"event_name", "event_version", "event_payload"}
	mockRows := sqlmock.NewRows(tableSchema).
		AddRow("event0", 2, eventStringGenerator(schema.MakeNewEvent("event0", 2), t))

	sqlmock.ExpectQuery("select").WillReturnRows(mockRows)

	mockEvent, err := mockBackend.NewestEvent("event0")
	if err != nil {
		t.Fatalf("an error '%s' was not expected when getting newest event", err)
	}

	expectedEvent := schema.MakeNewEvent("event0", 2)

	if !reflect.DeepEqual(mockEvent, expectedEvent) {
		t.Fatalf("Incorrect Row Response")
		fmt.Println(mockEvent)
		fmt.Println(expectedEvent)
	}

	sqlmock.ExpectQuery("select").WillReturnError(sql.ErrNoRows)
	mockEvent, err = mockBackend.NewestEvent("event3")

	if err != nil {
		t.Fatalf("an error '%s' was not expected when getting newest event that doesn't actually exist", err)
	}

	expectedEvent = schema.MakeNewEvent("event3", 1)

	if !reflect.DeepEqual(mockEvent, expectedEvent) {
		t.Fatalf("Incorrect Row Response")
		fmt.Println(mockEvent)
		fmt.Println(expectedEvent)
	}
}

func TestMockEventVersion(t *testing.T) {
	flag.Parse()

	db, err := sqlmock.New()

	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	mockBackend, err := NewPostgresBackend(db, testEventTable)
	if err != nil {
		t.Fatalf("an error '%s' was not expected when creating backend object and tested with ping", err)
	}

	defer db.Close()

	tableSchema := []string{"event_name", "event_version", "event_payload"}
	mockRows := sqlmock.NewRows(tableSchema).
		AddRow("event0", 1, eventStringGenerator(schema.MakeNewEvent("event0", 1), t))

	sqlmock.ExpectQuery("select").WillReturnRows(mockRows)

	mockEvent, err := mockBackend.EventVersion("event0", 1)
	if err != nil {
		t.Fatalf("an error '%s' was not expected when getting newest event", err)
	}

	expectedEvent := schema.MakeNewEvent("event0", 1)

	if !reflect.DeepEqual(mockEvent, expectedEvent) {
		t.Fatalf("Incorrect Row Response")
		fmt.Println(mockEvent)
		fmt.Println(expectedEvent)
	}

	sqlmock.ExpectQuery("select").WillReturnError(sql.ErrNoRows)
	mockEvent, err = mockBackend.EventVersion("event0", 3)

	if err == nil {
		t.Fatalf("An error was expected when getting newest event")
	}
}

func TestMockPutEvent(t *testing.T) {
	flag.Parse()

	db, err := sqlmock.New()

	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	mockBackend, err := NewPostgresBackend(db, testEventTable)
	if err != nil {
		t.Fatalf("an error '%s' was not expected when creating backend object and tested with ping", err)
	}

	defer db.Close()

	sqlmock.ExpectExec("insert").WillReturnResult(sqlmock.NewResult(0, 1))

	err = mockBackend.PutEvent(schema.MakeNewEvent("event2", 3))

	if err != nil {
		t.Fatalf("an error '%s' was not expected when getting newest event", err)
	}
}

func (pg *PostgresBackendObject) createTestTable() error {
	query := fmt.Sprintf(`create table %s (
                            event_name varchar(127) not null, 
                            event_version integer not null, 
                            event_payload JSONB, 
                            primary key (event_name, event_version));`, pq.QuoteIdentifier(pg.tableName))

	_, err := pg.connection.Exec(query)
	if err != nil {
		return errors.New("Could not add table to DB " + err.Error())
	}

	return nil
}

func (pg *PostgresBackendObject) dropTestTable() error {
	query := fmt.Sprintf(`drop table %s;`, pq.QuoteIdentifier(pg.tableName))

	_, err := pg.connection.Exec(query)
	if err != nil {
		return errors.New("Could not drop table from DB " + err.Error())
	}

	return nil
}
