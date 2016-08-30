package bpdb

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/twitchscience/blueprint/core"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
	"github.com/twitchscience/scoop_protocol/transformer"
)

var (
	maxColumns = 300
	keyNames   = []string{"distkey", "sortkey"}
)

// Bpdb is the interface of the blueprint db backend that stores schema state
type Bpdb interface {
	AllSchemas() ([]scoop_protocol.Config, error)
	Schema(name string) (*scoop_protocol.Config, error)
	UpdateSchema(*core.ClientUpdateSchemaRequest) error
	CreateSchema(*scoop_protocol.Config) error
	Migration(table string, to int) ([]*scoop_protocol.Operation, error)
}

func validateType(t string) error {
	for _, validType := range transformer.ValidTransforms {
		if validType == t {
			return nil
		}
	}
	return fmt.Errorf("type not found")
}

func validateIdentifier(name string) error {
	if len(name) < 1 || len(name) > 127 {
		return fmt.Errorf("must be between 1 and 127 characters, given length of %d", len(name))
	}
	matched, _ := regexp.MatchString(`^[A-Za-z_][A-Za-z0-9_-]*$`, name)
	if !matched {
		return fmt.Errorf("must begin with alpha or underscore and be composed of alphanumeric, underscore, or hyphen")
	}
	return nil
}

func validateIsNotKey(options string) error {
	for _, keyName := range keyNames {
		if strings.Contains(options, keyName) {
			return fmt.Errorf("this column is %s", keyName)
		}
	}
	return nil
}

func preValidateSchema(cfg *scoop_protocol.Config) error {
	err := validateIdentifier(cfg.EventName)
	if err != nil {
		return fmt.Errorf("event name invalid: %v", err)
	}
	for _, col := range cfg.Columns {
		err = validateIdentifier(col.OutboundName)
		if err != nil {
			return fmt.Errorf("column outbound name invalid: %v", err)
		}
		err := validateType(col.Transformer)
		if err != nil {
			return fmt.Errorf("column transformer invalid: %v", err)
		}
	}
	if len(cfg.Columns) >= maxColumns {
		return fmt.Errorf("too many columns, max is %d, given %d", maxColumns, len(cfg.Columns))
	}
	return nil
}

func requestToOps(req *core.ClientUpdateSchemaRequest) []scoop_protocol.Operation {
	ops := make([]scoop_protocol.Operation, len(req.Additions)+len(req.Deletes))
	i := 0
	for i, col := range req.Additions {
		ops[i] = scoop_protocol.Operation{
			Action: "add",
			Name:   col.OutboundName,
			ActionMetadata: map[string]string{
				"inbound":        col.InboundName,
				"column_type":    col.Transformer,
				"column_options": col.Length,
			},
		}
	}
	for j, colName := range req.Deletes {
		ops[i+j] = scoop_protocol.Operation{
			Action:         "delete",
			Name:           colName,
			ActionMetadata: map[string]string{},
		}
	}
	return ops
}

func preValidateUpdate(req *core.ClientUpdateSchemaRequest, bpdb Bpdb) error {
	schema, err := bpdb.Schema(req.EventName)
	if err != nil {
		return fmt.Errorf("error getting schema to validate schema update: %v", err)
	}

	// Validate schema "add"s
	for _, col := range req.Additions {
		err = validateIdentifier(col.OutboundName)
		if err != nil {
			return fmt.Errorf("column outbound name invalid: %v", err)
		}
		err = validateType(col.Transformer)
		if err != nil {
			return fmt.Errorf("column transformer invalid: %v", err)
		}
	}

	// Validate schema "delete"s
	for _, columnName := range req.Deletes {
		for _, existingCol := range schema.Columns {
			if existingCol.OutboundName == columnName {
				err = validateIsNotKey(existingCol.ColumnCreationOptions)
				if err != nil {
					return fmt.Errorf("column is a key and cannot be dropped: %v", err)
				}
				break // move on to next deleted column
			}
		}
	}

	ops := requestToOps(req)
	err = ApplyOperations(schema, ops)
	if err != nil {
		return err
	}

	if len(schema.Columns) > maxColumns {
		return fmt.Errorf("too many columns, max is %d, given %d adds and %d deletes, which would result in %d total", maxColumns, len(req.Additions), len(req.Deletes), len(schema.Columns))
	}
	return nil
}
