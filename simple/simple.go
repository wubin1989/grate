package simple

import (
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/wubin1989/grate"
)

// represents a set of data collections.
type simpleFile struct {
	filename string
	rows     [][]string
	iterRow  int
}

// List the individual data tables within this source.
func (t *simpleFile) List() ([]string, error) {
	return []string{filepath.Base(t.filename)}, nil
}

func (t *simpleFile) Close() error {
	return nil
}

// Get a Collection from the source by name.
func (t *simpleFile) Get(name string) (grate.Collection, error) {
	return t, nil
}

// Next advances to the next record of content.
// It MUST be called prior to any Scan().
func (t *simpleFile) Next() bool {
	t.iterRow++
	return t.iterRow < len(t.rows)
}

// Strings extracts values from the current record into a list of strings.
func (t *simpleFile) Strings() []string {
	return t.rows[t.iterRow]
}

// Formats extracts the format code for the current record into a list.
func (t *simpleFile) Formats() []string {
	res := make([]string, len(t.rows[t.iterRow]))
	for i := range res {
		res[i] = "General"
	}
	return res
}

// Types extracts the data types from the current record into a list.
// options: "boolean", "integer", "float", "string", "date",
// and special cases: "blank", "hyperlink" which are string types
func (t *simpleFile) Types() []string {
	res := make([]string, len(t.rows[t.iterRow]))
	for i, v := range t.rows[t.iterRow] {
		if v == "" {
			res[i] = "blank"
		} else {
			res[i] = "string"
		}
	}
	return res
}

// Scan extracts values from the current record into the provided arguments
// Arguments must be pointers to one of 5 supported types:
//     bool, int, float64, string, or time.Time
func (t *simpleFile) Scan(args ...interface{}) error {
	var err error
	row := t.rows[t.iterRow]
	if len(row) != len(args) {
		return fmt.Errorf("grate/simple: expected %d Scan destinations, got %d", len(row), len(args))
	}

	for i, a := range args {
		switch v := a.(type) {
		case *bool:
			switch strings.ToLower(row[i]) {
			case "1", "t", "true", "y", "yes":
				*v = true
			default:
				*v = false
			}
		case *int:
			var n int64
			n, err = strconv.ParseInt(row[i], 10, 64)
			*v = int(n)
		case *float64:
			*v, err = strconv.ParseFloat(row[i], 64)
		case *string:
			*v = row[i]
		case *time.Time:
			return errors.New("grate/simple: time.Time not supported, you must parse date strings manually")
		default:
			return grate.ErrInvalidScanType
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// IsEmpty returns true if there are no data values.
func (t *simpleFile) IsEmpty() bool {
	return len(t.rows) == 0
}

// Err returns the last error that occured.
func (t *simpleFile) Err() error {
	return nil
}
