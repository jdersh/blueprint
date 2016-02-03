package bpdb

import (
	"flag"
	"fmt"
	"reflect"
	"testing"

	"github.com/twitchscience/scoop_protocol/schema"

	"github.com/lib/pq"
)

var (
	postgresURL    = flag.String("postgresURL", "", "The login url for the postgres DB")
	useMockDB      = flag.Bool("useMockDB", true, "Whether you want to use the mock db for testing or not.  Defaults to true")
	testEventTable = "test_event_schemas"
	backend        Backend
)

func setupTestDB() (Backend, error) {
	flag.Parse()

	var err error

	if *useMockDB {
		backend, err = New("sqlite3", "./test_event_db.db", testEventTable)
		if err != nil {
			return Backend{}, fmt.Errorf("Could not extablish connection to test DB: %v", err)
		}
	} else {
		backend, err = New("postgres", *postgresURL, testEventTable)
		if err != nil {
			return Backend{}, fmt.Errorf("Could not extablish connection to DB: %v", err)
		}
	}

	if err != nil {
		return Backend{}, fmt.Errorf("Could not extablish connection to DB and store to postgres object: %v", err)
	}

	testEvents := []schema.Event{
		schema.MakeNewEvent("event_should_exist_0_max_ver_6", 0),
		schema.MakeNewEvent("event_should_exist_0_max_ver_6", 1),
		schema.MakeNewEvent("event_should_exist_0_max_ver_6", 2),
		schema.MakeNewEvent("event_should_exist_0_max_ver_6", 3),
		schema.MakeNewEvent("event_should_exist_0_max_ver_6", 4),
		schema.MakeNewEvent("event_should_exist_0_max_ver_6", 5),
		schema.MakeNewEvent("event_should_exist_0_max_ver_6", 6),

		schema.MakeNewEvent("event_should_exist_1_max_ver_1", 0),
		schema.MakeNewEvent("event_should_exist_1_max_ver_1", 1),

		schema.MakeNewEvent("event_should_exist_2_max_ver_4", 0),
		schema.MakeNewEvent("event_should_exist_2_max_ver_4", 1),
		schema.MakeNewEvent("event_should_exist_2_max_ver_4", 2),
		schema.MakeNewEvent("event_should_exist_2_max_ver_4", 3),
		schema.MakeNewEvent("event_should_exist_2_max_ver_4", 4),
	}

	backend.createTestTable()
	for _, testEvent := range testEvents {
		err := backend.PutEvent(testEvent)
		if err != nil {
			return Backend{}, fmt.Errorf("Could not add test events to DB: %v", err)
		}
	}

	return backend, nil
}

func DeepEqualChecker(expectedTestEvent, actualTestEvent schema.Event, t *testing.T) {
	if !reflect.DeepEqual(actualTestEvent, expectedTestEvent) {
		t.Errorf("Event grabbed from db was incorrect")
		t.Logf("Expected response: %+v", expectedTestEvent)
		t.Logf("Actual response: %+v", actualTestEvent)
	}
}

func TestEvents(t *testing.T) {
	backend, err := setupTestDB()
	if err != nil {
		t.Fatalf("Error %v setting up DB for test", err)
	}
	defer backend.connection.Close()
	defer backend.dropTestTable()

	newestTestEvents, err := backend.Events()
	if err != nil {
		t.Fatalf("Could not get events from db: %s", err)
	}

	expectedTestEvents := []schema.Event{
		schema.MakeNewEvent("event_should_exist_0_max_ver_6", 6),
		schema.MakeNewEvent("event_should_exist_1_max_ver_1", 1),
		schema.MakeNewEvent("event_should_exist_2_max_ver_4", 4),
	}

	//cannot use deepequals checker due to list arguments
	if !reflect.DeepEqual(newestTestEvents, expectedTestEvents) {
		t.Errorf("Events grabbed from db were incorrect")
		t.Logf("Expected response: %+v", expectedTestEvents)
		t.Logf("Actual response: %+v", newestTestEvents)
	}
}

func TestNewestEvent(t *testing.T) {
	backend, err := setupTestDB()
	if err != nil {
		t.Fatalf("Error %v setting up DB for test", err)
	}
	defer backend.connection.Close()
	defer backend.dropTestTable()

	newestTestEvent, err := backend.NewestEvent("event_should_exist_2_max_ver_4")
	if err != nil {
		t.Fatalf("Could not get event from db: %s", err)
	}

	expectedTestEvent := schema.MakeNewEvent("event_should_exist_2_max_ver_4", 4)

	DeepEqualChecker(expectedTestEvent, newestTestEvent, t)

	newestTestEvent, err = backend.NewestEvent("event_should_not_exist")
	if err == nil {
		t.Error("Should have errored as event should not have existed")
	}
}

func TestVersionedEventGeneric(t *testing.T) {
	backend, err := setupTestDB()
	if err != nil {
		t.Fatalf("Error %v setting up DB for test", err)
	}
	defer backend.connection.Close()
	defer backend.dropTestTable()

	specificTestEvent, err := backend.VersionedEvent("event_should_exist_2_max_ver_4", 1)
	if err != nil {
		t.Fatalf("Could not get event from db: %s", err)
	}

	expectedTestEvent := schema.MakeNewEvent("event_should_exist_2_max_ver_4", 1)

	DeepEqualChecker(expectedTestEvent, specificTestEvent, t)
}

func TestVersionedEventNonExistance(t *testing.T) {
	backend, err := setupTestDB()
	if err != nil {
		t.Fatalf("Error %v setting up DB for test", err)
	}
	defer backend.connection.Close()
	defer backend.dropTestTable()

	specificTestEvent, err := backend.VersionedEvent("event_should_not_exist", 4)
	if err == nil {
		t.Errorf("Should have errored due to event not existing but did not")
		t.Logf("Unexpected response: %+v", specificTestEvent)
	}
}

func TestVersionedEventNonVersion(t *testing.T) {
	backend, err := setupTestDB()
	if err != nil {
		t.Fatalf("Error %v setting up DB for test", err)
	}
	defer backend.connection.Close()
	defer backend.dropTestTable()

	specificTestEvent, err := backend.VersionedEvent("event_should_exist_1_max_ver_1", 2)
	if err == nil {
		t.Errorf("Should have errored due to event version not existing but did not")
		t.Logf("Unexpected response: %+v", specificTestEvent)
	}

}

func (b *Backend) createTestTable() error {
	query := fmt.Sprintf(`	create table %s (
                            	name varchar(127) not null, 
                            	version integer not null, 
                            	payload JSONB, 
                            	primary key (name, version));
								`, pq.QuoteIdentifier(b.tableName))

	_, err := b.connection.Exec(query)
	if err != nil {
		return fmt.Errorf("Could not add table to DB: %s", err)
	}

	query = fmt.Sprintf(`create index %s on %s (name asc);`, pq.QuoteIdentifier(b.tableName+"_name_asc"),
		pq.QuoteIdentifier(b.tableName))

	_, err = b.connection.Exec(query)
	if err != nil {
		return fmt.Errorf("Could not add index name on table %s: %v", b.tableName, err)
	}

	query = fmt.Sprintf(`create index %s on %s (version desc);`, pq.QuoteIdentifier(b.tableName+"_version_desc"),
		pq.QuoteIdentifier(b.tableName))

	_, err = b.connection.Exec(query)
	if err != nil {
		return fmt.Errorf("Could not add index version on table %s: %v", b.tableName, err)
	}

	return nil
}

func (b *Backend) dropTestTable() error {
	query := fmt.Sprintf(`drop table %s;`, pq.QuoteIdentifier(b.tableName))

	_, err := b.connection.Exec(query)
	if err != nil {
		return fmt.Errorf("Could not drop table from DB: %s", err)
	}

	return nil
}
