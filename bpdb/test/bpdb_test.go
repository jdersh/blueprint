package test

import (
	"database/sql"
	"flag"
	"fmt"
	"reflect"
	"testing"

	"github.com/twitchscience/blueprint/bpdb"
	"github.com/twitchscience/scoop_protocol/schema"
)

var (
	postgresURL    = flag.String("postgresURL", "", "The login url for the postgres DB")
	useMockDB      = flag.Bool("useMockDB", true, "Whether you want to use the mock db for testing or not.  Defaults to true")
	testEventTable = "test_event_schemas"
	backend        bpdb.Backend
)

func setupTestDB() (bpdb.Backend, *sql.DB, string, error) {
	flag.Parse()

	var driverName, urlName string

	if *useMockDB {
		driverName = "sqlite3"
		urlName = ":memory:"
	} else {
		driverName = "postgres"
		urlName = *postgresURL
	}

	connection, err := sql.Open(driverName, urlName)
	if err != nil {
		return bpdb.Backend{}, nil, "", fmt.Errorf("Could not extablish connection to DB: %v", err)
	}
	backend, err := bpdb.New(connection, testEventTable)

	testEvents := []schema.Event{
		schema.NewEvent("event_should_exist_0_max_ver_6", 0),
		schema.NewEvent("event_should_exist_0_max_ver_6", 1),
		schema.NewEvent("event_should_exist_0_max_ver_6", 2),
		schema.NewEvent("event_should_exist_0_max_ver_6", 3),
		schema.NewEvent("event_should_exist_0_max_ver_6", 4),
		schema.NewEvent("event_should_exist_0_max_ver_6", 5),
		schema.NewEvent("event_should_exist_0_max_ver_6", 6),

		schema.NewEvent("event_should_exist_1_max_ver_1", 0),
		schema.NewEvent("event_should_exist_1_max_ver_1", 1),

		schema.NewEvent("event_should_exist_2_max_ver_4", 0),
		schema.NewEvent("event_should_exist_2_max_ver_4", 1),
		schema.NewEvent("event_should_exist_2_max_ver_4", 2),
		schema.NewEvent("event_should_exist_2_max_ver_4", 3),
		schema.NewEvent("event_should_exist_2_max_ver_4", 4),
	}

	CreateTestTable(connection, testEventTable)
	for _, testEvent := range testEvents {
		err := backend.PutEvent(testEvent)
		if err != nil {
			return bpdb.Backend{}, nil, "", fmt.Errorf("Could not add test events to DB: %v", err)
		}
	}

	return backend, connection, testEventTable, nil
}

func DeepEqualChecker(expectedTestEvent, actualTestEvent []schema.Event, t *testing.T) {
	if !reflect.DeepEqual(actualTestEvent, expectedTestEvent) {
		t.Errorf("Event grabbed from db was incorrect")
		t.Logf("Expected response: %+v", expectedTestEvent)
		t.Logf("Actual response: %+v", actualTestEvent)
	}
}

func TestEvents(t *testing.T) {
	backend, connection, tableName, err := setupTestDB()
	if err != nil {
		t.Fatalf("Error %v setting up DB for test", err)
	}
	defer connection.Close()
	defer DropTestTable(connection, tableName)

	newestTestEvents, err := backend.Events()
	if err != nil {
		t.Fatalf("Could not get events from db: %s", err)
	}

	expectedTestEvents := []schema.Event{
		schema.NewEvent("event_should_exist_0_max_ver_6", 6),
		schema.NewEvent("event_should_exist_1_max_ver_1", 1),
		schema.NewEvent("event_should_exist_2_max_ver_4", 4),
	}

	//cannot use deepequals checker due to list arguments
	if !reflect.DeepEqual(newestTestEvents, expectedTestEvents) {
		t.Errorf("Events grabbed from db were incorrect")
		t.Logf("Expected response: %+v", expectedTestEvents)
		t.Logf("Actual response: %+v", newestTestEvents)
	}
}

func TestNewestEvent(t *testing.T) {
	backend, connection, tableName, err := setupTestDB()
	if err != nil {
		t.Fatalf("Error %v setting up DB for test", err)
	}
	defer connection.Close()
	defer DropTestTable(connection, tableName)

	newestTestEvent, err := backend.NewestEvent("event_should_exist_2_max_ver_4")
	if err != nil {
		t.Fatalf("Could not get event from db: %s", err)
	}

	expectedTestEvent := []schema.Event{schema.NewEvent("event_should_exist_2_max_ver_4", 4)}

	DeepEqualChecker(expectedTestEvent, newestTestEvent, t)

	newestTestEvent, err = backend.NewestEvent("event_should_not_exist")
	if err == nil {
		t.Error("Should have errored as event should not have existed")
	}
}

func TestVersionedEventGeneric(t *testing.T) {
	backend, connection, tableName, err := setupTestDB()
	if err != nil {
		t.Fatalf("Error %v setting up DB for test", err)
	}
	defer connection.Close()
	defer DropTestTable(connection, tableName)

	specificTestEvent, err := backend.VersionedEvent("event_should_exist_2_max_ver_4", 1)
	if err != nil {
		t.Fatalf("Could not get event from db: %s", err)
	}

	expectedTestEvent := []schema.Event{schema.NewEvent("event_should_exist_2_max_ver_4", 1)}

	DeepEqualChecker(expectedTestEvent, specificTestEvent, t)
}

func TestVersionedEventNonExistance(t *testing.T) {
	backend, connection, tableName, err := setupTestDB()
	if err != nil {
		t.Fatalf("Error %v setting up DB for test", err)
	}
	defer connection.Close()
	defer DropTestTable(connection, tableName)

	specificTestEvent, err := backend.VersionedEvent("event_should_not_exist", 4)
	if err == nil {
		t.Errorf("Should have errored due to event not existing but did not")
		t.Logf("Unexpected response: %+v", specificTestEvent)
	}
}

func TestVersionedEventNonVersion(t *testing.T) {
	backend, connection, tableName, err := setupTestDB()
	if err != nil {
		t.Fatalf("Error %v setting up DB for test", err)
	}
	defer connection.Close()
	defer DropTestTable(connection, tableName)

	specificTestEvent, err := backend.VersionedEvent("event_should_exist_1_max_ver_1", 2)
	if err == nil {
		t.Errorf("Should have errored due to event version not existing but did not")
		t.Logf("Unexpected response: %+v", specificTestEvent)
	}

}
