// Copyright 2020 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package spannerdriver

import (
	"cloud.google.com/go/spanner"
	"context"
	"database/sql"
	"os"
	"reflect"
	"testing"

	"net/http"
	_ "net/http/pprof"
	"log"
)

var (
	dsn string
)


func init() {

	var projectId, instanceId, databaseId string
	var ok bool

	// Get environment variables or set to default.
	if instanceId, ok = os.LookupEnv("SPANNER_TEST_INSTANCE"); !ok {
		instanceId = "test-instance"
	}
	if projectId, ok = os.LookupEnv("SPANNER_TEST_PROJECT"); !ok {
		projectId = "test-project"
	}
	if databaseId, ok = os.LookupEnv("SPANNER_TEST_DBID"); !ok {
		databaseId = "gotest"
	}

	// Derive data source name.
	dsn = "projects/" + projectId + "/instances/" + instanceId + "/databases/" + databaseId

	
}

func TestMain(m *testing.M){

	log.Printf("\n\nserting on port 8080\n\n")

	exitVal := m.Run()


	log.Fatal(http.ListenAndServe(":8080", nil))
	os.Exit(exitVal)
}

// Executes DML using the client library.
func ExecuteDMLClientLib(dml []string) error {

	// Open client/
	ctx := context.Background()
	client, err := spanner.NewClient(ctx, dsn)
	if err != nil {
		return err
	}
	defer client.Close()

	// Put strings into spanner.Statement structure.
	var stmts []spanner.Statement
	for _, line := range dml {
		stmts = append(stmts, spanner.NewStatement(line))
	}

	// Execute statements.
	_, err = client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		_, err := txn.BatchUpdate(ctx, stmts)
		if err != nil {
			return err
		}
		return nil
	})

	return err
}

func TestQueryContext(t *testing.T) {

	// Open db.
	ctx := context.Background()
	db, err := sql.Open("spanner", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Set up test table.
	_, err = db.ExecContext(ctx,
		`CREATE TABLE TestQueryContext (
			A   STRING(1024),
			B  STRING(1024),
			C   STRING(1024)
		)	 PRIMARY KEY (A)`)
	if err != nil {
		t.Fatal(err)
	}
	err = ExecuteDMLClientLib([]string{`INSERT INTO TestQueryContext (A, B, C) 
		VALUES ("a1", "b1", "c1"), ("a2", "b2", "c2") , ("a3", "b3", "c3") `})
	if err != nil {
		t.Fatal(err)
	}

	type testQueryContextRow struct {
		A, B, C string
	}

	tests := []struct {
		name           string
		input          string
		want           []testQueryContextRow
		wantErrorQuery bool
		wantErrorScan  bool
		wantErrorClose bool
	}{
		{
			name:           "empty query",
			wantErrorClose: true,
			input:          "",
			want:           []testQueryContextRow{},
		},
		{
			name:           "syntax error",
			wantErrorClose: true,
			input:          "SELECT SELECT * FROM TestQueryContext",
			want:           []testQueryContextRow{},
		},
		{
			name:  "return nothing",
			input: "SELECT * FROM TestQueryContext WHERE A = \"hihihi\"",
			want:  []testQueryContextRow{},
		},
		{
			name:  "select one tuple",
			input: "SELECT * FROM TestQueryContext WHERE A = \"a1\"",
			want: []testQueryContextRow{
				{A: "a1", B: "b1", C: "c1"},
			},
		},
		{
			name:  "select subset of tuples",
			input: "SELECT * FROM TestQueryContext WHERE A = \"a1\" OR A = \"a2\"",
			want: []testQueryContextRow{
				{A: "a1", B: "b1", C: "c1"},
				{A: "a2", B: "b2", C: "c2"},
			},
		},
		{
			name:  "select subset of tuples with !=",
			input: "SELECT * FROM TestQueryContext WHERE A != \"a3\"",
			want: []testQueryContextRow{
				{A: "a1", B: "b1", C: "c1"},
				{A: "a2", B: "b2", C: "c2"},
			},
		},
		{
			name:  "select entire table",
			input: "SELECT * FROM TestQueryContext ORDER BY A",
			want: []testQueryContextRow{
				{A: "a1", B: "b1", C: "c1"},
				{A: "a2", B: "b2", C: "c2"},
				{A: "a3", B: "b3", C: "c3"},
			},
		},
		{
			name:           "query non existent table",
			wantErrorClose: true,
			input:          "SELECT * FROM NonExistent",
			want:           []testQueryContextRow{},
		},
	}

	// Run tests
	for _, tc := range tests {

		rows, err := db.QueryContext(ctx, tc.input)
		if (err != nil) && (!tc.wantErrorQuery) {
			t.Errorf("%s: unexpected query error: %v", tc.name, err)
		}
		if (err == nil) && (tc.wantErrorQuery) {
			t.Errorf("%s: expected query error but error was %v", tc.name, err)
		}

		got := []testQueryContextRow{}
		for rows.Next() {
			var curr testQueryContextRow
			err := rows.Scan(&curr.A, &curr.B, &curr.C)
			if (err != nil) && (!tc.wantErrorScan) {
				t.Errorf("%s: unexpected query error: %v", tc.name, err)
			}
			if (err == nil) && (tc.wantErrorScan) {
				t.Errorf("%s: expected query error but error was %v", tc.name, err)
			}

			got = append(got, curr)
		}

		rows.Close()
		err = rows.Err()
		if (err != nil) && (!tc.wantErrorClose) {
			t.Errorf("%s: unexpected query error: %v", tc.name, err)
		}
		if (err == nil) && (tc.wantErrorClose) {
			t.Errorf("%s: expected query error but error was %v", tc.name, err)
		}
		if !reflect.DeepEqual(tc.want, got) {
			t.Errorf("Test failed: %s. want: %v, got: %v", tc.name, tc.want, got)
		}

	}

	// Drop table.
	_, err = db.ExecContext(ctx, `DROP TABLE TestQueryContext`)
	if err != nil {
		t.Error(err)
	}
}

func TestExecContextDdl(t *testing.T) {

	// Open db.
	ctx := context.Background()
	db, err := sql.Open("spanner", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tests := []struct {
		name, drop string
		input      string
		wantError  bool
	}{
		{
			name: "create table ok",
			drop: "DROP TABLE TestTable",
			input: `CREATE TABLE TestTable (
				A   STRING(1024),
				B  STRING(1024),
			)	 PRIMARY KEY (A)`,
		},
		{
			name: "create table name duplicate",
			input: `CREATE TABLE TestTable (
				A   STRING(1024),
				B  STRING(1024),
			)	 PRIMARY KEY (A)`,
			wantError: true,
		},
		{
			name: "create table syntax error",
			input: `CREATE CREATE TABLE SyntaxError (
				A   STRING(1024),
				B  STRING(1024),
				C   STRING(1024)
			)	 PRIMARY KEY (A)`,
			wantError: true,
		},
		{
			name: "create table no primary key",
			input: `CREATE TABLE NoPrimaryKey (
				A   STRING(1024),
				B  STRING(1024),
			)`,
			wantError: true,
		},
		{
			name: "create table float primary key",
			drop: "DROP TABLE FloatPrimaryKey",
			input: `CREATE TABLE FloatPrimaryKey (
				A   FLOAT64,
				B  STRING(1024),
			)	PRIMARY KEY (A)`,
		},
		{
			name: "create table bool primary key",
			drop: "DROP TABLE BoolPrimaryKey",
			input: `CREATE TABLE BoolPrimaryKey (
				A   BOOL,
				B  STRING(1024),
			)	PRIMARY KEY (A)`,
		},
		{
			name: "create table lowercase ddl",
			drop: "DROP TABLE LowerDdl",
			input: `create table LowerDdl (
				A   INT64,
				B  STRING(1024),
			)	PRIMARY KEY (A)`,
		},
		{
			name: "create table integer name",
			input: `CREATE TABLE 42 (
				A   INT64,
				B  STRING(1024),
			)	PRIMARY KEY (A)`,
			wantError: true,
		},
	}

	// Run tests.
	for _, tc := range tests {
		_, err = db.ExecContext(ctx, tc.input)
		if (err != nil) && (!tc.wantError) {
			t.Errorf("%s: unexpected query error: %v", tc.name, err)
		}
		if (err == nil) && (tc.wantError) {
			t.Errorf("%s: expected query error but error was %v", tc.name, err)
		}
	}

	// Remove any stray tables.
	for _, tc := range tests {
		if !tc.wantError {
			_, err = db.ExecContext(ctx, tc.drop)
			if err != nil {
				t.Error(err)
			}
		}
	}

}

/*
func TestExecContextDml(t *testing.T) {

	// Open db.
	ctx := context.Background()
	db, err := sql.Open("spanner", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Set up test table.
	conn, err := NewConnector()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	_, err = db.ExecContext(ctx,
		`CREATE TABLE TestDml (
			A   STRING(1024),
			B  STRING(1024),
		)	 PRIMARY KEY (A)`)
	if err != nil {
		t.Fatal(err)
	}


	// Insert.
	num, errr := db.ExecContext(ctx, `INSERT INTO TestDml (A, B) 
	VALUES ("a1", "b1"),("a12, "b2") `)
	if errr != nil {
		t.Fatal(errr)
	}
	fmt.Printf("\n\nNim: %+v\n", num)
	fmt.Println("XXXXXXXXXXXXXXXXXXX")
	fmt.Println(num.RowsAffected())

	t.Fatal(num)
}

*/