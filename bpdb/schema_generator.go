package bpdb

import (
	"fmt"

	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

// ApplyOperations applies the list of operations in order to the schema,
// migrating the schema to a new state
func ApplyOperations(s *scoop_protocol.Config, operations []scoop_protocol.Operation) error {
	for _, op := range operations {
		err := ApplyOperation(s, op)
		if err != nil {
			return err
		}
	}
	return nil
}

// ApplyOperation applies a single operation to the schema, migrating the
// schema to a new state
func ApplyOperation(s *scoop_protocol.Config, op scoop_protocol.Operation) error {
	switch op.Action {
	case "add":
		for _, existingCol := range s.Columns {
			if existingCol.OutboundName == op.Name {
				return fmt.Errorf("Outbound column '%s' already exists in schema, cannot add again.", op.Name)
			}
		}
		s.Columns = append(s.Columns, scoop_protocol.ColumnDefinition{
			InboundName:           op.ActionMetadata["inbound"],
			OutboundName:          op.Name,
			Transformer:           op.ActionMetadata["column_type"],
			ColumnCreationOptions: op.ActionMetadata["column_options"],
		})
	case "delete":
		for i, existingCol := range s.Columns {
			if existingCol.OutboundName == op.Name {
				// splice the dropped column away
				s.Columns = append(s.Columns[:i], s.Columns[i+1:]...)
				return nil
			}
		}
		return fmt.Errorf("Outbound column '%s' does not exists in schema, cannot drop non-existing column.", op.Name)
	case "rename":
		for i, existingCol := range s.Columns {
			if existingCol.OutboundName == op.Name {
				s.Columns[i].OutboundName = op.ActionMetadata["new_outbound"]
				return nil
			}
		}
		return fmt.Errorf("Outbound column '%s' does not exists in schema, cannot rename non-existent column.", op.Name)
	default:
		return fmt.Errorf("Error, unsupported operation action %s.", op.Action)
	}
	return nil
}
