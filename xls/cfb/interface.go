package cfb

import (
	"bytes"
	"fmt"
	"io"
	"io/fs"
	"os"
)

// Open a Compound File Binary Format document.
func Open(filename string) (*Document, error) {
	d := &Document{}
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	err = d.load(f)
	if err != nil {
		return nil, err
	}
	return d, nil
}

// OpenFile opens a Compound File Binary Format document from an fs.File.
func OpenFile(file fs.File) (*Document, error) {
	// Ensure the file implements io.ReadSeeker
	rs, ok := file.(io.ReadSeeker)
	if !ok {
		// If not a ReadSeeker, we'll read all contents and create a bytes.Reader
		data, err := io.ReadAll(file)
		if err != nil {
			return nil, err
		}
		rs = bytes.NewReader(data)
	}

	d := &Document{}
	err := d.load(rs)
	if err != nil {
		return nil, err
	}
	return d, nil
}

// OpenReader opens a Compound File Binary Format document from an io.ReadCloser.
func OpenReader(reader io.ReadCloser) (*Document, error) {
	// Read all data from the reader
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	
	// Close the reader since we've read all data
	if err := reader.Close(); err != nil {
		return nil, err
	}
	
	// Create a bytes.Reader that implements io.ReadSeeker
	rs := bytes.NewReader(data)
	
	d := &Document{}
	err = d.load(rs)
	if err != nil {
		return nil, err
	}
	return d, nil
}

// List the streams contained in the document.
func (d *Document) List() ([]string, error) {
	var res []string
	for _, e := range d.dir {
		if e.ObjectType == typeStream {
			res = append(res, e.String())
		}
	}
	return res, nil
}

// Open the named stream contained in the document.
func (d *Document) Open(name string) (io.ReadSeeker, error) {
	for _, e := range d.dir {
		if e.String() == name && e.ObjectType == typeStream {
			if e.StreamSize < uint64(d.header.MiniStreamCutoffSize) {
				return d.getMiniStreamReader(uint32(e.StartingSectorLocation), e.StreamSize)
			} else if e.StreamSize != 0 {
				return d.getStreamReader(uint32(e.StartingSectorLocation), e.StreamSize)
			}
		}
	}
	return nil, fmt.Errorf("cfb: stream '%s' not found", name)
}
