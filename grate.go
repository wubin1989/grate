// Package grate opens tabular data files (such as spreadsheets and delimited plaintext files)
// and allows programmatic access to the data contents in a consistent interface.
package grate

import (
	"errors"
	"io"
	"io/fs"
	"log"
	"sort"
)

// Source represents a set of data collections.
type Source interface {
	// List the individual data tables within this source.
	List() ([]string, error)

	// Get a Collection from the source by name.
	Get(name string) (Collection, error)

	// Close the source and discard memory.
	Close() error
}

// Collection represents an iterable collection of records.
type Collection interface {
	// Next advances to the next record of content.
	// It MUST be called prior to any Scan().
	Next() bool

	// Strings extracts values from the current record into a list of strings.
	Strings() []string

	// Types extracts the data types from the current record into a list.
	// options: "boolean", "integer", "float", "string", "date",
	// and special cases: "blank", "hyperlink" which are string types
	Types() []string

	// Formats extracts the format codes for the current record into a list.
	Formats() []string

	// Scan extracts values from the current record into the provided arguments
	// Arguments must be pointers to one of 5 supported types:
	//     bool, int64, float64, string, or time.Time
	// If invalid, returns ErrInvalidScanType
	Scan(args ...interface{}) error

	// IsEmpty returns true if there are no data values.
	IsEmpty() bool

	// Err returns the last error that occured.
	Err() error
}

// OpenFunc defines a Source's instantiation function.
// It should return ErrNotInFormat immediately if filename is not of the correct file type.
type OpenFunc func(filename string) (Source, error)

// OpenFileFunc defines a Source's instantiation function that works with fs.File.
// It should return ErrNotInFormat immediately if the file is not of the correct file type.
type OpenFileFunc func(file fs.File) (Source, error)

// OpenReaderFunc defines a Source's instantiation function that works with io.ReadCloser.
// It should return ErrNotInFormat immediately if the reader content is not of the correct file type.
type OpenReaderFunc func(reader io.ReadCloser) (Source, error)

// Open a tabular data file and return a Source for accessing it's contents.
func Open(filename string) (Source, error) {
	for _, o := range srcTable {
		src, err := o.op(filename)
		if err == nil {
			return src, nil
		}
		if !errors.Is(err, ErrNotInFormat) {
			return nil, err
		}
		if Debug {
			log.Println(" ", filename, "is not in", o.name, "format")
		}
	}
	return nil, ErrUnknownFormat
}

// OpenFile opens a tabular data file from an fs.File and returns a Source for accessing its contents.
func OpenFile(file fs.File) (Source, error) {
	for _, o := range fileTable {
		src, err := o.op(file)
		if err == nil {
			return src, nil
		}
		if !errors.Is(err, ErrNotInFormat) {
			return nil, err
		}
		if Debug {
			log.Println("file is not in", o.name, "format")
		}
	}
	return nil, ErrUnknownFormat
}

// OpenReader opens a tabular data file from an io.ReadCloser and returns a Source for accessing its contents.
func OpenReader(reader io.ReadCloser) (Source, error) {
	for _, o := range readerTable {
		src, err := o.op(reader)
		if err == nil {
			return src, nil
		}
		if !errors.Is(err, ErrNotInFormat) {
			return nil, err
		}
		if Debug {
			log.Println("reader is not in", o.name, "format")
		}
	}
	return nil, ErrUnknownFormat
}

type srcOpenTab struct {
	name string
	pri  int
	op   OpenFunc
}

type fileOpenTab struct {
	name string
	pri  int
	op   OpenFileFunc
}

type readerOpenTab struct {
	name string
	pri  int
	op   OpenReaderFunc
}

var srcTable = make([]*srcOpenTab, 0, 20)
var fileTable = make([]*fileOpenTab, 0, 20)
var readerTable = make([]*readerOpenTab, 0, 20)

// Register the named source as a grate datasource implementation.
func Register(name string, priority int, opener OpenFunc) error {
	if Debug {
		log.Println("Registering the", name, "format at priority", priority)
	}
	srcTable = append(srcTable, &srcOpenTab{name: name, pri: priority, op: opener})
	sort.Slice(srcTable, func(i, j int) bool {
		return srcTable[i].pri < srcTable[j].pri
	})
	return nil
}

// RegisterFile registers the named source as a grate datasource implementation for fs.File.
func RegisterFile(name string, priority int, opener OpenFileFunc) error {
	if Debug {
		log.Println("Registering the", name, "format for fs.File at priority", priority)
	}
	fileTable = append(fileTable, &fileOpenTab{name: name, pri: priority, op: opener})
	sort.Slice(fileTable, func(i, j int) bool {
		return fileTable[i].pri < fileTable[j].pri
	})
	return nil
}

// RegisterReader registers the named source as a grate datasource implementation for io.ReadCloser.
func RegisterReader(name string, priority int, opener OpenReaderFunc) error {
	if Debug {
		log.Println("Registering the", name, "format for io.ReadCloser at priority", priority)
	}
	readerTable = append(readerTable, &readerOpenTab{name: name, pri: priority, op: opener})
	sort.Slice(readerTable, func(i, j int) bool {
		return readerTable[i].pri < readerTable[j].pri
	})
	return nil
}

const (
	// ContinueColumnMerged marks a continuation column within a merged cell.
	ContinueColumnMerged = "→"
	// EndColumnMerged marks the last column of a merged cell.
	EndColumnMerged = "⇥"

	// ContinueRowMerged marks a continuation row within a merged cell.
	ContinueRowMerged = "↓"
	// EndRowMerged marks the last row of a merged cell.
	EndRowMerged = "⤓"
)
