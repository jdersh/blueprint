package bpdb

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/twitchscience/scoop_protocol/schema"

	"github.com/lib/pq"
)

type PostgresBackendObject struct {
	connection *sql.DB
	tableName  string
}

type eventRow struct {
	eventName    string
	eventVersion int
	eventPayload string
}

func BuildPostgresBackend(db *sql.DB, tableName string) (PostgresBackendObject, error) {
	err := db.Ping()
	if err != nil {
		return PostgresBackendObject{}, fmt.Errorf("Error %v establishing connection to FB", err)
	}

	return PostgresBackendObject{
		connection: db,
		tableName:  tableName,
	}, nil
}

func (pg *PostgresBackendObject) GetEvents() ([]schema.Event, error) {
	jsonEvents, err := pg.getItems()
	if err != nil {
		return []schema.Event{}, fmt.Errorf("Error %v getting events from database", err)
	}

	var events []schema.Event

	for _, jsonEvent := range jsonEvents {

		var event schema.Event

		err := json.Unmarshal(jsonEvent, &event)
		if err != nil {
			return []schema.Event{}, fmt.Errorf("Error %v unmarshalling json: %s", err, string(jsonEvent))
		}

		events = append(events, event)
	}

	return events, nil
}

func (pg *PostgresBackendObject) GetNewestEvent(eventName string) (schema.Event, error) {
	jsonEvent, err := pg.getNewestItem(eventName)
	if err != nil {
		return schema.Event{}, fmt.Errorf("Error %v getting newest event from DB", err)
	}

	var event schema.Event
	err = json.Unmarshal(jsonEvent, &event)
	if err != nil {
		return schema.Event{}, fmt.Errorf("Error %v unmarshalling newest event json: %s ", err, string(jsonEvent))
	}
	return event, nil
}

func (pg *PostgresBackendObject) GetSpecificEvent(eventName string, eventVersion int) (schema.Event, error) {
	jsonEvent, err := pg.getSpecificItem(eventName, eventVersion)
	if err != nil {
		return schema.Event{}, fmt.Errorf("Error %v getting specific event from DB", err)
	}

	var event schema.Event
	err = json.Unmarshal(jsonEvent, &event)
	if err != nil {
		return schema.Event{}, fmt.Errorf("Error %v unmarshalling specific event json: %s", err, string(jsonEvent))
	}
	return event, nil
}

func (pg *PostgresBackendObject) UpdateEvent(event schema.Event) error {
	eventName := event.Name
	eventVersion := event.Version
	eventPayload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("Error %v marshalling event", err)
	}

	err = pg.putItem(eventName, eventVersion, eventPayload)
	if err != nil {
		return fmt.Errorf("Error %v loading event into DB", err)
	}

	return nil
}

func (pg *PostgresBackendObject) getItems() ([][]byte, error) {
	//'distinct' removes all duplicate rows from the result set, on the column or group of columns it is called on.
	//'order by' lets you chose which row will be kepy and which rows will be removed.
	query := fmt.Sprintf(`	select distinct on (event_name)
                    			event_name,
                    			event_version,
                    			event_payload
                 			from %s 
                 			order by event_name, event_version desc;`, pq.QuoteIdentifier(pg.tableName))

	rawEventRows, err := pg.connection.Query(query)
	if err != nil {
		return nil, fmt.Errorf("Error %v querying newest items from DB", err)
	}

	defer rawEventRows.Close()

	var byteEvents [][]byte

	for rawEventRows.Next() {
		var stringEventRow eventRow

		err := rawEventRows.Scan(
			&stringEventRow.eventName,
			&stringEventRow.eventVersion,
			&stringEventRow.eventPayload)

		if err != nil {
			return nil, fmt.Errorf("Error %v storing rows from DB", err)
		}

		byteEventPayload := []byte(stringEventRow.eventPayload)
		byteEvents = append(byteEvents, byteEventPayload)
	}

	return byteEvents, nil
}

func (pg *PostgresBackendObject) getNewestItem(eventName string) ([]byte, error) {
	query := fmt.Sprintf(`select event_name, event_version, event_payload from %s
                where event_name = $1
                order by event_version desc limit 1;`, pq.QuoteIdentifier(pg.tableName))

	var stringEventRow eventRow
	err := pg.connection.QueryRow(query, eventName).Scan(
		&stringEventRow.eventName,
		&stringEventRow.eventVersion,
		&stringEventRow.eventPayload)

	switch {
	case err == sql.ErrNoRows:
		temp, err := json.Marshal(schema.MakeNewEvent(eventName, 1))
		if err != nil {
			return nil, fmt.Errorf("Error %v marshalling json for new event", err)
		}
		return temp, nil
	case err != nil:
		return nil, fmt.Errorf("Error %v querying newest item from DB", err)
	}

	return []byte(stringEventRow.eventPayload), nil
}

func (pg *PostgresBackendObject) getSpecificItem(eventName string, eventVersion int) ([]byte, error) {
	query := fmt.Sprintf(`select event_name, event_version, event_payload from %s
                where event_name = $1 and event_version = $2
                order by event_version desc limit 1;`, pq.QuoteIdentifier(pg.tableName))

	var stringEventRow eventRow
	err := pg.connection.QueryRow(query, eventName, eventVersion).Scan(
		&stringEventRow.eventName,
		&stringEventRow.eventVersion,
		&stringEventRow.eventPayload)

	switch {
	case err == sql.ErrNoRows:
		return nil, fmt.Errorf("Specific item does not exist in DB: %v", err)
	case err != nil:
		return nil, fmt.Errorf("Error %v querying specific item from DB", err)
	}

	return []byte(stringEventRow.eventPayload), nil
}

func (pg *PostgresBackendObject) putItem(eventName string, eventVersion int, eventPayload []byte) error {
	query := fmt.Sprintf(`insert into %s values ($1, $2, $3);`, pq.QuoteIdentifier(pg.tableName))

	_, err := pg.connection.Exec(query, eventName, eventVersion, string(eventPayload))
	if err != nil {
		return fmt.Errorf("Error %v executing insert into DB", err)
	}

	return nil
}
