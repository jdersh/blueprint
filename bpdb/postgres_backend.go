package bpdb

import (
	"database/sql"
	"errors"
	"fmt"
	"log"

	"encoding/json"

	"github.com/lib/pq"
	"github.com/twitchscience/blueprint/core"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

var (
	schemaQuery = `
SELECT event, action, name, version, ordering, action_metadata
FROM operation
WHERE event = $1
ORDER BY version ASC, ordering ASC
`
	allSchemasQuery = `
SELECT event, action, name,  version, ordering, action_metadata
FROM operation
ORDER BY version ASC, ordering ASC
`
	migrationQuery = `
SELECT action, name, action_metadata
FROM operation
WHERE version = $1
AND event = $2
ORDER BY version ASC, ordering ASC
`
	addColumnQuery = `INSERT INTO operation
(event, action, name, version, ordering, action_metadata)
VALUES ($1, $2, $3, $4, $5, $6)
`
	nextVersionQuery = `SELECT max(version) + 1
FROM operation
WHERE event = $1
GROUP BY event`
)

type postgresBackend struct {
	db *sql.DB
}

type operationRow struct {
	event          string
	action         string
	name           string
	actionMetadata map[string]string
	version        int
	ordering       int
}

// NewPostgresBackend creates a postgres bpdb backend to interface with
// the schema store
func NewPostgresBackend(dbConnection string) (Bpdb, error) {
	db, err := sql.Open("postgres", dbConnection)
	if err != nil {
		return nil, fmt.Errorf("Got err %v while connecting to db.", err)
	}
	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("Got err %v trying to ping the db.", err)
	}
	b := &postgresBackend{db: db}
	return b, nil
}

func (p *postgresBackend) Migration(table string, to int) ([]*scoop_protocol.Operation, error) {
	rows, err := p.db.Query(migrationQuery, to, table)
	if err != nil {
		return nil, fmt.Errorf("Error querying for migration (%s) to v%v: %v.", table, to, err)
	}
	ops := []*scoop_protocol.Operation{}
	defer func() {
		err := rows.Close()
		if err != nil {
			log.Printf("Error closing rows in postgres backend Migration: %v", err)
		}
	}()
	for rows.Next() {
		var op scoop_protocol.Operation
		var b []byte
		err := rows.Scan(&op.Action, &op.Name, &b)
		if err != nil {
			return nil, fmt.Errorf("Error parsing row into Operation: %v.", err)
		}

		err = json.Unmarshal(b, &op.ActionMetadata)
		if err != nil {
			return nil, fmt.Errorf("Error unmarshalling action_metadata: %v.", err)
		}
		ops = append(ops, &op)
	}
	return ops, nil
}

func execAddColumns(reqEvent string, reqData []core.Column, tx *sql.Tx, newVersion int, additionOffset int) error {
	for i, col := range reqData {
		metadata := metadataMarshaller{
			Inbound:       col.InboundName,
			ColumnType:    col.Transformer,
			ColumnOptions: col.Length,
		}
		b, err := json.Marshal(metadata)
		if err != nil {
			return fmt.Errorf("Error marshalling add column metadata json: %v", err)
		}
		_, err = tx.Exec(addColumnQuery,
			reqEvent,
			"add",
			col.OutboundName,
			newVersion,
			additionOffset+i,
			b,
		)
		if err != nil {
			rollErr := tx.Rollback()
			if rollErr != nil {
				return fmt.Errorf("Error rolling back commit: %v.", rollErr)
			}
			return fmt.Errorf("Error INSERTing row for add column on %s: %v", reqEvent, err)
		}
	}
	return nil
}
func execDeleteColumns(reqEvent string, deletes []string, tx *sql.Tx, newVersion int, additionOffset int) error {
	for i, col := range deletes {
		_, err := tx.Exec(addColumnQuery,
			reqEvent,
			"delete",
			col,
			newVersion,
			additionOffset+i,
			"{}",
		)
		if err != nil {
			rollErr := tx.Rollback()
			if rollErr != nil {
				return fmt.Errorf("Error rolling back commit: %v.", rollErr)
			}
			return fmt.Errorf("Error INSERTing row for delete column on %s: %v", reqEvent, err)
		}
	}
	return nil
}

func (p *postgresBackend) UpdateSchema(req *core.ClientUpdateSchemaRequest) error {
	err := preValidateUpdate(req, p)
	if err != nil {
		return fmt.Errorf("Invalid schema creation request: %v", err)
	}

	tx, err := p.db.Begin()
	if err != nil {
		return fmt.Errorf("Error beginning transaction for schema update: %v.", err)
	}

	row := tx.QueryRow(nextVersionQuery, req.EventName)
	var newVersion int
	err = row.Scan(&newVersion)
	if err != nil {
		return fmt.Errorf("Error parsing response for version number for %s: %v.", req.EventName, err)
	}

	err = execAddColumns(req.EventName, req.Additions, tx, newVersion, 0)
	if err != nil {
		return err
	}

	err = execDeleteColumns(req.EventName, req.Deletes, tx, newVersion, len(req.Additions))
	if err != nil {
		return err
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("Error commiting schema update for %s: %v", req.EventName, err)
	}
	return nil
}

type metadataMarshaller struct {
	Inbound       string `json:"inbound"`
	ColumnType    string `json:"column_type"`
	ColumnOptions string `json:"column_options"`
}

func (p *postgresBackend) CreateSchema(req *scoop_protocol.Config) error {
	err := preValidateSchema(req)
	if err != nil {
		return fmt.Errorf("Invalid schema creation request: %v", err)
	}

	tx, err := p.db.Begin()
	if err != nil {
		return fmt.Errorf("Error beginning transaction for schema creation: %v.", err)
	}

	for i, col := range req.Columns {
		metadata := metadataMarshaller{
			Inbound:       col.InboundName,
			ColumnType:    col.Transformer,
			ColumnOptions: col.ColumnCreationOptions,
		}
		var b []byte
		b, err = json.Marshal(metadata)
		if err != nil {
			return fmt.Errorf("Error marshalling metadata json")
		}

		_, err = tx.Exec(addColumnQuery,
			req.EventName,
			"add",
			col.OutboundName,
			b,
			0, // version = 0 since new schema
			i,
		)
		if err != nil {
			rollErr := tx.Rollback()
			if rollErr != nil {
				return fmt.Errorf("Error rolling back commit: %v.", rollErr)
			}
			if pqErr, ok := err.(*pq.Error); ok {
				if pqErr.Code.Name() == "unique_violation" { // pkey violation, meaning table already exists
					return errors.New("table already exists")
				}
			}
			return fmt.Errorf("Error INSERTing row for new column on %s: %v", req.EventName, err)
		}
	}
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("Error commiting schema creation for %s: %v", req.EventName, err)
	}
	return nil
}

// scanOperationRows scans the rows into operationRow objects
func scanOperationRows(rows *sql.Rows) ([]operationRow, error) {
	ops := []operationRow{}
	defer func() {
		err := rows.Close()
		if err != nil {
			log.Printf("Error closing rows in postgres backend Migration: %v", err)
		}
	}()
	for rows.Next() {
		var op operationRow
		var b []byte
		err := rows.Scan(&op.event, &op.action, &op.name, &op.version, &op.ordering, &b)
		if err != nil {
			return nil, fmt.Errorf("Error parsing operation row: %v.", err)
		}
		err = json.Unmarshal(b, &op.actionMetadata)
		if err != nil {
			return nil, fmt.Errorf("Error unmarshalling action_metadata: %v.", err)
		}
		ops = append(ops, op)
	}
	return ops, nil
}

// Schema returns the current schema for the table `name`
func (p *postgresBackend) Schema(name string) (*scoop_protocol.Config, error) {
	rows, err := p.db.Query(schemaQuery, name)
	if err != nil {
		return nil, fmt.Errorf("Error querying for schema %s: %v.", name, err)
	}
	ops, err := scanOperationRows(rows)
	if err != nil {
		return nil, err
	}

	schemas, err := generateSchemas(ops)
	if err != nil {
		return nil, fmt.Errorf("Internal state bad - Error generating schemas from operations: %v", err)
	}
	if len(schemas) > 1 {
		return nil, fmt.Errorf("Expected only one schema, received %v.", len(schemas))
	}
	if len(schemas) == 0 {
		return nil, fmt.Errorf("Unable to find schema: %v", name)
	}
	return &schemas[0], nil
}

// Schema returns all of the current schemas
func (p *postgresBackend) AllSchemas() ([]scoop_protocol.Config, error) {
	rows, err := p.db.Query(allSchemasQuery)
	if err != nil {
		return nil, fmt.Errorf("Error querying for all schemas: %v.", err)
	}
	ops, err := scanOperationRows(rows)
	if err != nil {
		return nil, err
	}
	return generateSchemas(ops)
}

// max returns the max of the two arguments
func max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

// generateSchemas creates schemas from a list of operations
// by applying the operations in the order they appear in the array
func generateSchemas(ops []operationRow) ([]scoop_protocol.Config, error) {
	schemas := make(map[string]*scoop_protocol.Config)
	for _, op := range ops {
		_, exists := schemas[op.event]
		if !exists {
			schemas[op.event] = &scoop_protocol.Config{EventName: op.event}
		}
		err := ApplyOperation(schemas[op.event], scoop_protocol.Operation{
			Action:         op.action,
			ActionMetadata: op.actionMetadata,
			Name:           op.name,
		})
		if err != nil {
			return []scoop_protocol.Config{}, fmt.Errorf("Error applying operation to schema: %v", err)
		}
		schemas[op.event].Version = max(schemas[op.event].Version, op.version)
	}
	ret := make([]scoop_protocol.Config, len(schemas))

	i := 0
	for _, val := range schemas {
		ret[i] = *val
		i++
	}
	return ret, nil
}
