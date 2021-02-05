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

// Executes DML using the client library.
func ExecuteDMLClientLib(dml []string) error {

	// Open client.
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
	if _, err = db.ExecContext(ctx, `DROP TABLE TestQueryContext`); err != nil {
		t.Error(err)
	}
}

// note: isDdl function does not check validity of statement
// just that the statement begins with a DDL instruction.
// Other checking performed by database.
func TestIsDdl(t *testing.T) {

	tests := []struct {
		name  string
		input string
		want  bool
	}{
		{
			name: "valid create",
			input: `CREATE TABLE Valid (
				A   STRING(1024)
			)	 PRIMARY KEY (A)`,
			want: true,
		},
		{
			name: "leading spaces",
			input: `    CREATE TABLE Valid (
				A   STRING(1024)
			)	 PRIMARY KEY (A)`,
			want: true,
		},
		{
			name: "leading newlines",
			input: `
			CREATE TABLE Valid (
				A   STRING(1024)
			)	 PRIMARY KEY (A)`,
			want: true,
		},
		{
			name: "leading tabs",
			input: `		CREATE TABLE Valid (
				A   STRING(1024)
			)	 PRIMARY KEY (A)`,
			want: true,
		},
		{
			name: "leading whitespace, miscellaneous",
			input: `
							 
			 CREATE TABLE Valid (
				A   STRING(1024)
			)	 PRIMARY KEY (A)`,
			want: true,
		},
		{
			name: "lower case",
			input: `create table Valid (
				A   STRING(1024)
			)	 PRIMARY KEY (A)`,
			want: true,
		},
		{
			name: "mixed case, leading whitespace",
			input: ` 
			 cREAte taBLE Valid (
				A   STRING(1024)
			)	 PRIMARY KEY (A)`,
			want: true,
		},
		{
			name:  "insert (not ddl)",
			input: `INSERT INTO Valid`,
			want:  false,
		},
		{
			name:  "delete (not ddl)",
			input: `DELETE FROM Valid`,
			want:  false,
		},
		{
			name:  "update (not ddl)",
			input: `UPDATE Valid`,
			want:  false,
		},
		{
			name:  "drop",
			input: `DROP TABLE Valid`,
			want:  true,
		},
		{
			name:  "alter",
			input: `alter TABLE Valid`,
			want:  true,
		},
		{
			name:  "typo (ccreate)",
			input: `cCREATE TABLE Valid`,
			want:  false,
		},
		{
			name:  "typo (reate)",
			input: `REATE TABLE Valid`,
			want:  false,
		},
		{
			name:  "typo (rx ceate)",
			input: `x CREATE TABLE Valid`,
			want:  false,
		},
		{
			name:  "leading int",
			input: `0CREATE TABLE Valid`,
			want:  false,
		},
	}

	for _, tc := range tests {
		got, err := isDdl(tc.input)
		if err != nil {
			t.Error(err)
		}
		if got != tc.want {
			t.Errorf("isDdl test failed, %s: wanted %t got %t.", tc.name, tc.want, got)
		}
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

	// Run tests.
	tests := []struct {
		name, drop string
		input      string
		wantError  bool
	}{
		{
			name: "create table ok",
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
		{
			name: "drop table ok",
			input: "DROP TABLE TestTable",
		},
		{
			name: "drop non existent table",
			input: "DROP TABLE NonExistent",
			wantError: true,
		},
		// Build and fill table to test refferential integrity.
		// No cascade, foreign key.
		{
			name: "ref ingegrity no cascade create parent table",
			input: `CREATE TABLE ParentNoCascade (
				id   INT64,
			)	PRIMARY KEY (id)`,
		},
		{
			name: "ref ingegrity no cascade create child table",
			input: `CREATE TABLE ChildNoCascade (
				id   INT64,
				parent_id	INT64,
				CONSTRAINT fk_nc FOREIGN KEY (parent_id) REFERENCES ParentNoCascade (id)
			)	PRIMARY KEY (id)`,
		},
		{
			name: "ref ingegrity no cascade fill parent table",
			input: `INSERT INTO  ParentNoCascade (id) VALUES (1), (2)`,
		},
		{
			name: "ref ingegrity no cascade fill child table",
			input: `INSERT INTO  ChildNoCascade (id, parent_id) VALUES (2, 1), (4, 2)`,
		},
		// Cascade, interleave.
		{
			name: "ref ingegrity no cascade create parent table",
			input: `CREATE TABLE ParentCascade (
				parent_id   INT64,
			)	PRIMARY KEY (parent_id)`,
		},
		{
			name: "ref ingegrity no cascade create child table",
			input: `CREATE TABLE ChildCascade (
				parent_id	INT64,
				id   INT64,
			)	PRIMARY KEY (parent_id, id), 
			INTERLEAVE IN PARENT ParentCascade ON DELETE CASCADE`,
		},
		{
			name: "ref ingegrity no cascade fill parent table",
			input: `INSERT INTO  ParentCascade (parent_id) VALUES (1), (2)`,
		},
		{
			name: "ref ingegrity no cascade fill child table",
			input: `INSERT INTO  ChildCascade (id, parent_id) VALUES (2, 1), (4, 2)`,
		},
		// Tests for referential integrity. 
		{
			name: "drop table referential integrity violation no cascade",
			input: "DROP TABLE ParentNoCascade",
			wantError: true,
		},
		{
			name: "drop table referential integrity violation cascade",
			input: "DROP TABLE ParentCascade",
			wantError: true,
		},
		// Clean up referential integrity tables in the correct order. 
		// No cascade.
		{
			name: "ref integrity clean up ChildNoCascade",
			input: "DROP TABLE ChildNoCascade",
		},
		{
			name: "ref integrity clean up ParentNoCascade",
			input: "DROP TABLE ParentNoCascade",
		},
		// Cascade
		{
			name: "ref integrity clean up ChildCascade",
			input: "DROP TABLE ChildCascade",
		},
		{
			name: "ref integrity clean up ParentCascade",
			input: "DROP TABLE ParentCascade",
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
		if tc.drop != "" {
			_, err = db.ExecContext(ctx, tc.drop)
			if err != nil {
				t.Error(err)
			}
		}
	}
}


func TestExecContextDdlReferential(t *testing.T) {



}