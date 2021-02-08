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
	"context"
	"database/sql"
	"testing"
)


func TestNullScan(t *testing.T) {

	// Open db.
	ctx := context.Background()
	db, err := sql.Open("spanner", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Set up test table.
	_, err = db.ExecContext(ctx,
		`CREATE TABLE TestNullScan (
			key	STRING(1024),
			testString	STRING(1024),
			testBytes	BYTES(1024),
			testInt	INT64,
			testFloat	FLOAT64,
			testBool	BOOL
		) PRIMARY KEY (key)`)
	if err != nil {
		t.Fatal(err)
	}

	// Fill with nulls.
	_, err = db.ExecContext(ctx, 
		`INSERT INTO  TestNullScan
		(key, testString, testBytes, testInt, testFloat, testBool)
		VALUES ('nullstring', null, CAST("nullstring" as bytes), 42, 42, true )`,
	) 
	if err != nil {
		t.Fatal(err)
	}

	type TestNullScanRow struct {
		key        string
		testString string
		testBytes  []byte
		testInt    int
		testFloat  float64
		testBool   bool
	}
	

	tests := []struct {
		name      string
		input     string
		want []TestNullScanRow
		wantErrorQuery bool
		wantErrorScan bool
		wantErrorClose bool
	}{
		{
			// Should possibly give scan error instead of filling w null string
			name: "read null string",
			input: `SELECT * FROM TestNullScan WHERE key = "nullstring"`,
			want:           []testQueryContextRow{
				{key: "nullstring", testString: "", testBytes: , testInt: , testFloat: , testBool: },
			},

		},

	}


	for _, tc := range tests {
		
		// Run query
		rows, err := db.QueryContext(ctx, tc.input)
		if (err != nil) && (!tc.wantErrorQuery) {
			t.Errorf("%s: unexpected query error: %v", tc.name, err)
		}
		if (err == nil) && (tc.wantErrorQuery) {
			t.Errorf("%s: expected query error but error was %v", tc.name, err)
		}

		got := []TestNullScanRow {}
		for rows.Next() {
			var curr TestNullScanRow 
			err := rows.Scan(&curr.key, &curr.testString, &curr.testBytes, &curr.testInt, &curr.testFloat, &curr.testBool)
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

		/*
		if !reflect.DeepEqual(tc.want, got) {
			t.Errorf("Test failed: %s. want: %v, got: %v", tc.name, tc.want, got)
		}*/

		t.Fatal(got)

	}


}