package api

import (
	"database/sql"
	"flag"
	"fmt"
	"testing"

	"github.com/twitchscience/blueprint/bpdb"
	bpdbTest "github.com/twitchscience/blueprint/bpdb/test"

	//schemaTest "github.com/twitchscience/scoop_protocol/schema/test"
)

var (
	testEventTable = "test_event_schemas"
	backend        bpdb.Backend
	testServer     server
)

func setupTestDB() (bpdb.Backend, *sql.DB, string, error) {
	flag.Parse()

	driverName := "sqlite3"
	urlName := ":memory:"

	connection, err := sql.Open(driverName, urlName)
	if err != nil {
		return bpdb.Backend{}, nil, "", fmt.Errorf("Could not extablish connection to DB: %v", err)
	}
	backend, err := bpdb.New(connection, testEventTable)

	bpdbTest.CreateTestTable(connection, testEventTable)

	return backend, connection, testEventTable, nil
}

func setupTestServer() (*server, *sql.DB, string, error) {
	backend, connection, tableName, err := setupTestDB()

	server := &server{backend: backend}
	return server, connection, tableName, err
}

func TestSchemas(t *testing.T) {
	server, connection, tableName, err := setupTestServer()
	if err != nil {
		t.Error("could not setup test, sql connection did not open")
	}

	defer connection.Close()
	defer bpdbTest.DropTestTable(connection, tableName)

}

func TestSchema(t *testing.T) {
	server, connection, tableName, err := setupTestServer()
	if err != nil {
		t.Error("could not setup test, sql connection did not open")
	}

	defer connection.Close()
	defer bpdbTest.DropTestTable(connection, tableName)

}

func TestDeleteSchema(t *testing.T) {
	server, connection, tableName, err := setupTestServer()
	if err != nil {
		t.Error("could not setup test, sql connection did not open")
	}

	defer connection.Close()
	defer bpdbTest.DropTestTable(connection, tableName)

}

func TestUpdateSchema(t *testing.T) {
	server, connection, tableName, err := setupTestServer()
	if err != nil {
		t.Error("could not setup test, sql connection did not open")
	}

	defer connection.Close()
	defer bpdbTest.DropTestTable(connection, tableName)

}
