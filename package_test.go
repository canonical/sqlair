package sqlair_test

import (
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	. "gopkg.in/check.v1"

	"github.com/canonical/sqlair"
)

// Hook up gocheck into the "go test" runner.
func TestExpr(t *testing.T) { TestingT(t) }

type PackageSuite struct{}

var _ = Suite(&PackageSuite{})

func setupDB() (*sql.DB, error) {
	return sql.Open("sqlite3", ":memory:")
}

func createExampleDB(create string, inserts []string) (*sql.DB, error) {
	db, err := setupDB()
	if err != nil {
		return nil, err
	}

	_, err = db.Exec(create)
	if err != nil {
		return nil, err
	}
	for _, insert := range inserts {
		_, err := db.Exec(insert)
		if err != nil {
			return nil, err
		}
	}

	return db, nil
}

type JujuLeaseKey struct {
	Namespace string `db:"type"`
	ModelUUID string `db:"model_uuid"`
	Lease     string `db:"name"`
}

type JujuLeaseInfo struct {
	Holder string `db:"holder"`
	Expiry int    `db:"expiry"`
}

func JujuStoreLeaseDB() (string, *sql.DB, error) {
	create := `
CREATE TABLE lease (
	model_uuid text,
	name text,
	holder text,
	expiry integer,
	lease_type_id text
);
CREATE TABLE lease_type (
	id text,
	type text
);

`
	drop := `
	 drop table lease;
	 drop table lease_type;
	 `

	inserts := []string{
		"INSERT INTO lease VALUES ('uuid1', 'name1', 'holder1', 1, 'type_id1');",
		"INSERT INTO lease VALUES ('uuid2', 'name2', 'holder2', 4, 'type_id1');",
		"INSERT INTO lease VALUES ('uuid3', 'name3', 'holder3', 7, 'type_id2');",
		"INSERT INTO lease_type VALUES ('type_id1', 'type1');",
		"INSERT INTO lease_type VALUES ('type_id2', 'type2');",
	}

	db, err := createExampleDB(create, inserts)
	if err != nil {
		return "", nil, err
	}
	return drop, db, nil

}

func (s *PackageSuite) TestJujuStore(c *C) {
	var tests = []struct {
		summery  string
		query    string
		types    []any
		inputs   []any
		outputs  [][]any
		expected [][]any
	}{{
		summery: "juju store lease group query",
		query: `
SELECT (t.type, l.model_uuid, l.name) AS &JujuLeaseKey.*, (l.holder, l.expiry) AS &JujuLeaseInfo.*
FROM   lease l JOIN lease_type t ON l.lease_type_id = t.id
WHERE  t.type = $JujuLeaseKey.type
AND    l.model_uuid = $JujuLeaseKey.model_uuid`,
		types:    []any{JujuLeaseKey{}, JujuLeaseInfo{}},
		inputs:   []any{JujuLeaseKey{Namespace: "type1", ModelUUID: "uuid1"}},
		outputs:  [][]any{{&JujuLeaseKey{}, &JujuLeaseInfo{}}},
		expected: [][]any{{&JujuLeaseKey{Namespace: "type1", ModelUUID: "uuid1", Lease: "name1"}, &JujuLeaseInfo{Holder: "holder1", Expiry: 1}}},
	}}

	drop, db, err := JujuStoreLeaseDB()
	if err != nil {
		c.Fatal(err)
	}

	sqlairDB := sqlair.NewDB(db)

	for _, t := range tests {

		stmt, err := sqlair.Prepare(t.query, t.types...)
		if err != nil {
			c.Errorf("test %q failed (Prepare):\ninput: %s\nerr: %s\n", t.summery, t.query, err)
			continue
		}

		q, err := sqlairDB.Query(stmt, t.inputs...)
		if err != nil {
			c.Errorf("test %q failed (Query):\ninput: %s\nerr: %s\n", t.summery, t.query, err)
			continue
		}

		i := 0
		for q.Next() {
			if i > len(t.outputs) {
				c.Errorf("test %q failed (Next):\ninput: %s\nerr: more rows that expected\n", t.summery, t.query)
				break
			}
			if !q.Decode(t.outputs[i]...) {
				c.Errorf("test %q failed (Decode):\ninput: %s\nerr: %s\n", t.summery, t.query, q.Err)
				break
			}
			i++
		}

		err = q.Close()
		if err != nil {
			c.Errorf("test %q failed (Close):\ninput: %s\nerr: %s\n", t.summery, t.query, err)
		}
	}

	_, err = sqlairDB.Exec(sqlair.MustPrepare(drop))
	if err != nil {
		c.Fatal(err)
	}
}
