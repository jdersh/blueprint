package PostgresClient

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/twitchscience/scoop_protocol/schema"

	"errors"

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
		return PostgresBackendObject{}, errors.New("Could not check link to DB: " + err.Error())
	}

	return PostgresBackendObject{
		connection: db,
		tableName:  tableName,
	}, nil
}

func (pg *PostgresBackendObject) GetEvents() ([]schema.Event, error) {
	jsonEvents, err := pg.getItems()
	if err != nil {
		return []schema.Event{}, errors.New("Failed to get events from database: " + err.Error())
	}

	var events []schema.Event
	var event schema.Event

	for _, jsonEvent := range jsonEvents {

		err := json.Unmarshal(jsonEvent, &event)
		if err != nil {
			return []schema.Event{}, errors.New("Unable to unmarshal json data into Events. Possibly incorrect json formatting: " + string(jsonEvent))
		}

		events = append(events, event)
	}

	return events, nil
}

func (pg *PostgresBackendObject) GetNewestEvent(eventName string) (schema.Event, error) {
	jsonEvent, err := pg.getNewestItem(eventName)
	if err != nil {
		return schema.Event{}, errors.New("Failed to get specific event from database: " + err.Error())
	}

	var event schema.Event
	err = json.Unmarshal(jsonEvent, &event)
	if err != nil {
		return schema.Event{}, errors.New("Unable to unmarshal json data into Event. Possibly incorrect json formatting.")
	}
	return event, nil
}

func (pg *PostgresBackendObject) GetSpecificEvent(eventName string, eventVersion int) (schema.Event, error) {
	jsonEvent, err := pg.getSpecificItem(eventName, eventVersion)
	if err != nil {
		return schema.Event{}, errors.New("Failed to get specific event from database: " + err.Error())
	}

	var event schema.Event
	err = json.Unmarshal(jsonEvent, &event)
	if err != nil {
		return schema.Event{}, errors.New("Unable to unmarshal json data into Event. Possibly incorrect json formatting.")
	}
	return event, nil
}

func (pg *PostgresBackendObject) UpdateEvent(event schema.Event) error {
	eventName := event.Name
	eventVersion := event.Version
	eventPayload, err := json.Marshal(event)
	if err != nil {
		return errors.New("Unable to marshal event as json.")
	}

	err = pg.putItem(eventName, eventVersion, eventPayload)
	if err != nil {
		return errors.New("An error occured trying to load the event into the DB: " + err.Error())
	}

	return nil
}

func (pg *PostgresBackendObject) getItems() ([][]byte, error) {
	query := fmt.Sprintf(`select distinct on (event_name)
                    event_name,
                    event_version,
                    event_payload
                 from %s 
                 order by event_name, event_version desc;`, pq.QuoteIdentifier(pg.tableName))

	rawEventRows, err := pg.connection.Query(query)
	if err != nil {
		return nil, errors.New("Could not get items from db: " + err.Error())
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
			return nil, errors.New("Items from DB were not what was expected: " + err.Error())
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
			return nil, errors.New("Could not create json for new event: " + err.Error())
		}
		return temp, nil
	case err != nil:
		return nil, errors.New("Problem occured querying event from db: " + err.Error())
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
		return nil, errors.New("Event with specific version does not exist in DB")
	case err != nil:
		return nil, errors.New("Problem occured querying event from db: " + err.Error())
	}

	return []byte(stringEventRow.eventPayload), nil
}

func (pg *PostgresBackendObject) putItem(eventName string, eventVersion int, eventPayload []byte) error {
	query := fmt.Sprintf(`insert into %s values ($1, $2, $3);`, pq.QuoteIdentifier(pg.tableName))

	_, err := pg.connection.Exec(query, eventName, eventVersion, string(eventPayload))
	if err != nil {
		return errors.New("Could not add new event into DB: " + err.Error())
	}

	return nil
}

func (pg *PostgresBackendObject) createTestTable() error {
	query := fmt.Sprintf(`create table %s (
                            event_name varchar(127) not null, 
                            event_version integer not null, 
                            event_payload JSONB);`, pq.QuoteIdentifier(pg.tableName))

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
