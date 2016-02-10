package test

import (
	"database/sql"
	"fmt"

	"github.com/lib/pq"
)

func CreateTestTable(connection *sql.DB, tableName string) error {
	query := fmt.Sprintf(`  create table %s (
                                name varchar(127) not null, 
                                version integer not null, 
                                payload JSONB, 
                                primary key (name, version));
                                `, pq.QuoteIdentifier(tableName))

	_, err := connection.Exec(query)
	if err != nil {
		return fmt.Errorf("Could not add table to DB: %s", err)
	}

	query = fmt.Sprintf(`create index %s on %s (name asc);`, pq.QuoteIdentifier(tableName+"_name_asc"),
		pq.QuoteIdentifier(tableName))

	_, err = connection.Exec(query)
	if err != nil {
		return fmt.Errorf("Could not add index name on table %s: %v", tableName, err)
	}

	query = fmt.Sprintf(`create index %s on %s (version desc);`, pq.QuoteIdentifier(tableName+"_version_desc"),
		pq.QuoteIdentifier(tableName))

	_, err = connection.Exec(query)
	if err != nil {
		return fmt.Errorf("Could not add index version on table %s: %v", tableName, err)
	}

	return nil
}

func DropTestTable(connection *sql.DB, tableName string) error {
	query := fmt.Sprintf(`drop table %s;`, pq.QuoteIdentifier(tableName))

	_, err := connection.Exec(query)
	if err != nil {
		return fmt.Errorf("Could not drop table from DB: %s", err)
	}

	return nil
}
