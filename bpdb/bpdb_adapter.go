package bpdb

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/twitchscience/scoop_protocol/schema"

	"github.com/lib/pq"
	//necessary for testing
	_ "github.com/mattn/go-sqlite3"
)

//Backend stores the connection object and tableName necessary to store events in DB
type Backend struct {
	connection *sql.DB
	tableName  string
}

type eventRow struct {
	name    string
	version int
	payload string
}

//New creates a new Backend object for the DB. Establishes connection to the DB
func New(connection *sql.DB, tableName string) (Backend, error) {

	err := connection.Ping()
	if err != nil {
		return Backend{}, fmt.Errorf("Error '%v' establishing connection to DB", err)
	}

	return Backend{
		connection: connection,
		tableName:  tableName,
	}, nil
}

//Events returns the newest version of each event in the DB
func (b *Backend) Events() ([]schema.Event, error) {
	jsonEvents, err := b.items()
	if err != nil {
		return nil, err
	}

	var events []schema.Event

	for _, jsonEvent := range jsonEvents {
		var event schema.Event

		err := json.Unmarshal([]byte(jsonEvent), &event)
		if err != nil {
			return nil, fmt.Errorf("Error '%v' unmarshalling json: %s", err, jsonEvent)
		}

		events = append(events, event)
	}

	return events, nil
}

//NewestEvent returns the single newest event specified
func (b *Backend) NewestEvent(name string) ([]schema.Event, error) {
	jsonEvent, err := b.newestItem(name)
	if err != nil {
		return nil, err
	}

	var event schema.Event
	err = json.Unmarshal([]byte(jsonEvent), &event)
	if err != nil {
		return nil, fmt.Errorf("Error '%v' unmarshalling newest event json: %s ", err, jsonEvent)
	}
	return []schema.Event{event}, nil
}

//VersionedEvent returns the event in the DB with the specified version
func (b *Backend) VersionedEvent(name string, version int) ([]schema.Event, error) {
	jsonEvent, err := b.versionedItem(name, version)
	if err != nil {
		return nil, err
	}

	var event schema.Event
	err = json.Unmarshal([]byte(jsonEvent), &event)
	if err != nil {
		return nil, fmt.Errorf("Error '%v' unmarshalling specific event json: %s", err, jsonEvent)
	}
	return []schema.Event{event}, nil
}

//PutEvent stores the passed in event into the DB
func (b *Backend) PutEvent(event schema.Event) error {
	eventPayload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("Error '%v' marshalling event", err)
	}

	err = b.putItem(event.EventName, event.Version, string(eventPayload))
	if err != nil {
		return fmt.Errorf("Error '%v' loading event into DB", err)
	}

	return nil
}

func (b *Backend) items() ([]string, error) {
	query := fmt.Sprintf(`	
		select schemas.name, schemas.version, schemas.payload 
		from (select name, max(version) as max_version 
			from %s group by name) versions
		join %s schemas
		on versions.name = schemas.name AND 
			versions.max_version = schemas.version;`, pq.QuoteIdentifier(b.tableName),
		pq.QuoteIdentifier(b.tableName))

	rawEventRows, err := b.connection.Query(query)
	if err != nil {
		return nil, fmt.Errorf("Error '%v' querying newest items from DB", err)
	}

	var events []string

	for rawEventRows.Next() {
		var row eventRow
		err := rawEventRows.Scan(
			&row.name,
			&row.version,
			&row.payload)

		if err != nil {
			return nil, fmt.Errorf("Error '%v' storing rows from DB", err)
		}

		events = append(events, row.payload)
	}

	err = rawEventRows.Close()
	if err != nil {
		return nil, err
	}

	return events, nil
}

func (b *Backend) newestItem(name string) (string, error) {
	query := fmt.Sprintf(`
		select name, version, payload 
		from %s
        where name = $1
        order by version desc limit 1;`, pq.QuoteIdentifier(b.tableName))

	var row eventRow
	err := b.connection.QueryRow(query, name).Scan(
		&row.name,
		&row.version,
		&row.payload)

	switch {
	case err == sql.ErrNoRows:
		return "", err
	case err != nil:
		return "", fmt.Errorf("Error '%v' querying newest item from DB", err)
	}

	return row.payload, nil
}

func (b *Backend) versionedItem(name string, version int) (string, error) {
	query := fmt.Sprintf(`
		select name, version, payload 
		from %s
        where name = $1 and version = $2
        order by version desc limit 1;`, pq.QuoteIdentifier(b.tableName))

	var row eventRow
	err := b.connection.QueryRow(query, name, version).Scan(
		&row.name,
		&row.version,
		&row.payload)

	switch {
	case err == sql.ErrNoRows:
		return "", err
	case err != nil:
		return "", fmt.Errorf("Error '%v' querying specific item from DB", err)
	}

	return row.payload, nil
}

func (b *Backend) putItem(name string, version int, payload string) error {
	query := fmt.Sprintf(`insert into %s values ($1, $2, $3);`, pq.QuoteIdentifier(b.tableName))

	_, err := b.connection.Exec(query, name, version, payload)
	if err != nil {
		return fmt.Errorf("Error '%v' executing insert into DB", err)
	}

	return nil
}
