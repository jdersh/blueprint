package bpdb

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/twitchscience/blueprint/core"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
	"github.com/twitchscience/scoop_protocol/transformer"
)

// redshiftReservedWords from http://docs.aws.amazon.com/redshift/latest/dg/r_pg_keywords.html
var redshiftReservedWords = []string{"AES128", "AES256", "ALL", "ALLOWOVERWRITE", "ANALYSE", "ANALYZE", "AND",
	"ANY", "ARRAY", "AS", "ASC", "AUTHORIZATION", "BACKUP", "BETWEEN", "BINARY", "BLANKSASNULL", "BOTH",
	"BYTEDICT", "BZIP2", "CASE", "CAST", "CHECK", "COLLATE", "COLUMN", "CONSTRAINT", "CREATE", "CREDENTIALS",
	"CROSS", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", "CURRENT_USER_ID", "DEFAULT",
	"DEFERRABLE", "DEFLATE", "DEFRAG", "DELTA", "DELTA32K", "DESC", "DISABLE", "DISTINCT", "DO", "ELSE",
	"EMPTYASNULL", "ENABLE", "ENCODE", "ENCRYPT", "ENCRYPTION", "END", "EXCEPT", "EXPLICIT", "FALSE",
	"FOR", "FOREIGN", "FREEZE", "FROM", "FULL", "GLOBALDICT256", "GLOBALDICT64K", "GRANT", "GROUP", "GZIP",
	"HAVING", "IDENTITY", "IGNORE", "ILIKE", "IN", "INITIALLY", "INNER", "INTERSECT", "INTO", "IS", "ISNULL",
	"JOIN", "LEADING", "LEFT", "LIKE", "LIMIT", "LOCALTIME", "LOCALTIMESTAMP", "LUN", "LUNS", "LZO", "LZOP",
	"MINUS", "MOSTLY13", "MOSTLY32", "MOSTLY8", "NATURAL", "NEW", "NOT", "NOTNULL", "NULL", "NULLS", "OFF",
	"OFFLINE", "OFFSET", "OLD", "ON", "ONLY", "OPEN", "OR", "ORDER", "OUTER", "OVERLAPS", "PARALLEL", "PARTITION",
	"PERCENT", "PERMISSIONS", "PLACING", "PRIMARY", "RAW", "READRATIO", "RECOVER", "REFERENCES", "RESPECT",
	"REJECTLOG", "RESORT", "RESTORE", "RIGHT", "SELECT", "SESSION_USER", "SIMILAR", "SOME", "SYSDATE", "SYSTEM",
	"TABLE", "TAG", "TDES", "TEXT255", "TEXT32K", "THEN", "TIMESTAMP", "TO", "TOP", "TRAILING", "TRUE",
	"TRUNCATECOLUMNS", "UNION", "UNIQUE", "USER", "USING", "VERBOSE", "WALLET", "WHEN", "WHERE", "WITH",
	"WITHOUT"}
var maxColumns = 300

// Operation represents a single change to a schema
type Operation struct {
	action        string
	inbound       string
	outbound      string
	columnType    string
	columnOptions string
}

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
	matched, _ := regexp.MatchString(`^[A-Za-z_][A-Za-z_-]*$`, name)
	if !matched {
		return fmt.Errorf("must begin with alpha or underscore and be composed of alphanumeric, underscore, or hyphen")
	}
	upper := strings.ToUpper(name)
	for _, reservedWord := range redshiftReservedWords {
		if upper == reservedWord {
			return fmt.Errorf("%s is a redshift reserved word", reservedWord)
		}
	}
	return nil
}

func preValidateSchema(cfg *scoop_protocol.Config) error {
	err := validateIdentifier(cfg.EventName)
	if err != nil {
		return fmt.Errorf("Event name invalid, %v", err)
	}
	for _, col := range cfg.Columns {
		err = validateIdentifier(col.OutboundName)
		if err != nil {
			return fmt.Errorf("Column outbound name invalid, %v", err)
		}
		err := validateType(col.Transformer)
		if err != nil {
			return fmt.Errorf("Column transformer invalid, %v", err)
		}
	}
	if len(cfg.Columns) >= maxColumns {
		return fmt.Errorf("Too many columns, max is %d, given %d", maxColumns, len(cfg.Columns))
	}
	return nil
}

func preValidateUpdate(req *core.ClientUpdateSchemaRequest, bpdb Bpdb) error {
	for _, col := range req.Columns {
		err := validateIdentifier(col.OutboundName)
		if err != nil {
			return fmt.Errorf("Column outbound name invalid, %v", err)
		}
		err = validateType(col.Transformer)
		if err != nil {
			return fmt.Errorf("Column transformer invalid, %v", err)
		}
	}
	schema, err := bpdb.Schema(req.EventName)
	if err != nil {
		return fmt.Errorf("Error getting schema to validate schema update: %v", err)
	}
	for _, col := range req.Columns {
		err = ApplyOperation(schema, Operation{
			action:        "add",
			inbound:       col.InboundName,
			outbound:      col.OutboundName,
			columnType:    col.Transformer,
			columnOptions: col.Length,
		})
		if err != nil {
			return fmt.Errorf("Error applying operations to table: %v", err)
		}
	}
	if len(schema.Columns) > maxColumns {
		return fmt.Errorf("Too many columns, max is %d, given %d new, which would result in %d total.", maxColumns, len(req.Columns), len(schema.Columns))
	}
	return nil
}
