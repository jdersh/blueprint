package PostgresClient

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"reflect"
	"testing"

	"github.com/twitchscience/scoop_protocol/schema"

	"github.com/DATA-DOG/go-sqlmock"
)

var (
	postgresURL        = flag.String("postgresURL", "", "The login url for the postgres DB")
	postgresMockStatus = flag.Bool("pgMockStatus", true, "Whether you want to use the mock db for testing or not.  Defaults to true")
	testEventTable     = "test_event_schemas"
	pgBackend          PostgresBackendObject
)

func TestGetEvents(t *testing.T) {
	flag.Parse()

	if *postgresMockStatus {
		t.SkipNow()
	}

	db, err := sql.Open("postgres", *postgresURL)
	if err != nil {
		t.Fatalf("Could not extablish connection to DB: %s", err)
	}

	defer db.Close()

	pgBackend, err := BuildPostgresBackend(db, testEventTable)
	if err != nil {
		t.Fatalf("Could not extablish connection to DB and store to postgres object: %s", err)
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
	defer pgBackend.dropTestTable()

	for _, testEvent := range testEvents {
		err := pgBackend.UpdateEvent(testEvent)
		if err != nil {
			t.Fatalf("Could not add test events to DB: %s", err)
		}
	}

	newestTestEvents, err := pgBackend.GetEvents()
	if err != nil {
		t.Fatalf("Could not get events from db: %s", err)
	}

	expectedTestEvents := []schema.Event{
		schema.MakeNewEvent("event0", 2),
		schema.MakeNewEvent("event1", 2),
		schema.MakeNewEvent("event2", 2),
	}

	if !reflect.DeepEqual(newestTestEvents, expectedTestEvents) {
		t.Errorf("Events grabbed from db were incorrect")
		t.Logf("Expected response: %+v", expectedTestEvents)
		t.Logf("Actual response: %+v", newestTestEvents)
	}
}

func TestGetNewestEvent(t *testing.T) {
	flag.Parse()

	if *postgresMockStatus {
		t.SkipNow()
	}

	db, err := sql.Open("postgres", *postgresURL)
	if err != nil {
		t.Fatalf("Could not extablish connection to DB: %s", err)
	}

	defer db.Close()

	pgBackend, err := BuildPostgresBackend(db, testEventTable)
	if err != nil {
		t.Fatalf("Could not extablish connection to DB and store to postgres object: %s", err)
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
	defer pgBackend.dropTestTable()

	for _, testEvent := range testEvents {
		err := pgBackend.UpdateEvent(testEvent)
		if err != nil {
			t.Fatalf("Could not add test events to DB: %s", err)
		}
	}

	newestTestEvent, err := pgBackend.GetNewestEvent("event2")
	if err != nil {
		t.Fatalf("Could not get event from db: %s", err)
	}

	expectedTestEvent := schema.MakeNewEvent("event2", 2)

	if !reflect.DeepEqual(newestTestEvent, expectedTestEvent) {
		t.Errorf("event grabbed from db was incorrect")
		t.Logf("Expected response: %+v", expectedTestEvent)
		t.Logf("Actual response: %+v", newestTestEvent)
	}

	newTestEvent, err := pgBackend.GetNewestEvent("event3")
	if err != nil {
		t.Fatalf("Could not get event from db: %s", err)
	}

	expectedTestEvent = schema.MakeNewEvent("event3", 1)

	if !reflect.DeepEqual(newTestEvent, expectedTestEvent) {
		t.Errorf("event grabbed from db was incorrect")
		t.Logf("Expected response: %+v", expectedTestEvent)
		t.Logf("Actual response: %+v", newTestEvent)
	}
}

func TestGetSpecificEvent(t *testing.T) {
	flag.Parse()

	if *postgresMockStatus {
		t.SkipNow()
	}

	db, err := sql.Open("postgres", *postgresURL)
	if err != nil {
		t.Fatalf("Could not extablish connection to DB: %s", err)
	}

	defer db.Close()

	pgBackend, err := BuildPostgresBackend(db, testEventTable)
	if err != nil {
		t.Fatalf("Could not extablish connection to DB and store to postgres object: %s", err)
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
	defer pgBackend.dropTestTable()

	for _, testEvent := range testEvents {
		err := pgBackend.UpdateEvent(testEvent)
		if err != nil {
			t.Fatalf("Could not add test events to DB: %s", err)
		}
	}

	specificTestEvent, err := pgBackend.GetSpecificEvent("event2", 1)
	if err != nil {
		t.Fatalf("Could not get event from db: %s", err)
	}

	expectedTestEvent := schema.MakeNewEvent("event2", 1)

	if !reflect.DeepEqual(specificTestEvent, expectedTestEvent) {
		t.Errorf("event grabbed from db was incorrect")
		t.Logf("Expected response: %+v", expectedTestEvent)
		t.Logf("Actual response: %+v", specificTestEvent)
	}

	specificTestEvent, err = pgBackend.GetSpecificEvent("event3", 1)
	if err == nil {
		t.Errorf("Should have errored due to event not existing but did not")
		t.Logf("Unexpected response: %+v", specificTestEvent)
	}

	specificTestEvent, err = pgBackend.GetSpecificEvent("event2", 3)
	if err == nil {
		t.Errorf("Should have errored due to event version not existing but did not")
		t.Logf("Unexpected response: %+v", specificTestEvent)
	}

}

func eventStringGenerator(event schema.Event) string {
	jsonEvent, _ := json.Marshal(event)
	return string(jsonEvent)
}

func TestMockGetEvents(t *testing.T) {
	flag.Parse()

	if !*postgresMockStatus { //Skip if you want a real DB
		t.SkipNow()
	}

	db, err := sqlmock.New() //Creates sql mock db

	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	mockBackend, err := BuildPostgresBackend(db, testEventTable) //build pg backend with mock db
	if err != nil {
		t.Fatalf("an error '%s' was not expected when creating backend object and tested with ping", err)
	}

	defer db.Close()

	tableSchema := []string{"event_name", "event_version", "event_payload"}
	mockRows := sqlmock.NewRows(tableSchema).
		AddRow("event0", 2, eventStringGenerator(schema.MakeNewEvent("event0", 2))).
		AddRow("event1", 2, eventStringGenerator(schema.MakeNewEvent("event1", 2))).
		AddRow("event2", 2, eventStringGenerator(schema.MakeNewEvent("event2", 2)))

	sqlmock.ExpectQuery("select").WillReturnRows(mockRows)

	allMockEvents, err := mockBackend.GetEvents()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when getting all newest events", err)
	}

	expectedEvents := []schema.Event{schema.MakeNewEvent("event0", 2), schema.MakeNewEvent("event1", 2), schema.MakeNewEvent("event2", 2)}

	if !reflect.DeepEqual(allMockEvents, expectedEvents) {
		t.Fatalf("Incorrect Row Response")
		fmt.Println(allMockEvents)
		fmt.Println(expectedEvents)
	}

}

func TestMockGetNewestEvent(t *testing.T) {
	flag.Parse()

	if !*postgresMockStatus { //Skip if you want a real DB
		t.SkipNow()
	}

	db, err := sqlmock.New() //Creates sql mock db

	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	mockBackend, err := BuildPostgresBackend(db, testEventTable) //build pg backend with mock db
	if err != nil {
		t.Fatalf("an error '%s' was not expected when creating backend object and tested with ping", err)
	}

	defer db.Close()

	tableSchema := []string{"event_name", "event_version", "event_payload"}
	mockRows := sqlmock.NewRows(tableSchema).
		AddRow("event0", 2, eventStringGenerator(schema.MakeNewEvent("event0", 2)))

	sqlmock.ExpectQuery("select").WillReturnRows(mockRows)

	mockEvent, err := mockBackend.GetNewestEvent("event0")
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
	mockEvent, err = mockBackend.GetNewestEvent("event3")

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

func TestMockGetSpecificEvent(t *testing.T) {
	flag.Parse()

	if !*postgresMockStatus { //Skip if you want a real DB
		t.SkipNow()
	}

	db, err := sqlmock.New() //Creates sql mock db

	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	mockBackend, err := BuildPostgresBackend(db, testEventTable) //build pg backend with mock db
	if err != nil {
		t.Fatalf("an error '%s' was not expected when creating backend object and tested with ping", err)
	}

	defer db.Close()

	tableSchema := []string{"event_name", "event_version", "event_payload"}
	mockRows := sqlmock.NewRows(tableSchema).
		AddRow("event0", 1, eventStringGenerator(schema.MakeNewEvent("event0", 1)))

	sqlmock.ExpectQuery("select").WillReturnRows(mockRows)

	mockEvent, err := mockBackend.GetSpecificEvent("event0", 1)
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
	mockEvent, err = mockBackend.GetSpecificEvent("event0", 3)

	if err == nil {
		t.Fatalf("An error was expected when getting newest event")
	}
}

func TestMockUpdateEvent(t *testing.T) {
	flag.Parse()

	if !*postgresMockStatus { //Skip if you want a real DB
		t.SkipNow()
	}

	db, err := sqlmock.New() //Creates sql mock db

	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	mockBackend, err := BuildPostgresBackend(db, testEventTable) //build pg backend with mock db
	if err != nil {
		t.Fatalf("an error '%s' was not expected when creating backend object and tested with ping", err)
	}

	defer db.Close()

	sqlmock.ExpectExec("insert").WillReturnResult(sqlmock.NewResult(0, 1))

	err = mockBackend.UpdateEvent(schema.MakeNewEvent("event2", 3))

	if err != nil {
		t.Fatalf("an error '%s' was not expected when getting newest event", err)
	}
}
