package xlsx

import (
	"archive/zip"
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/wubin1989/grate"
	"github.com/wubin1989/grate/commonxl"
)

var _ = grate.Register("xlsx", 5, Open)
var _ = grate.RegisterFile("xlsx", 5, OpenFile)
var _ = grate.RegisterReader("xlsx", 5, OpenReader)

// Document contains an Office Open XML document.
type Document struct {
	filename   string
	f          io.Closer
	r          *zip.Reader
	primaryDoc string

	// type => id => filename
	rels    map[string]map[string]string
	sheets  []*Sheet
	strings []string
	xfs     []uint16
	fmt     commonxl.Formatter
}

func (d *Document) Close() error {
	d.xfs = d.xfs[:0]
	d.xfs = nil
	d.strings = d.strings[:0]
	d.strings = nil
	d.sheets = d.sheets[:0]
	d.sheets = nil
	if d.f != nil {
		return d.f.Close()
	}
	return nil
}

func Open(filename string) (grate.Source, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	z, err := zip.NewReader(f, info.Size())
	if err != nil {
		return nil, grate.WrapErr(err, grate.ErrNotInFormat)
	}
	d := &Document{
		filename: filename,
		f:        f,
		r:        z,
	}

	err = d.init()
	if err != nil {
		d.Close()
		return nil, err
	}

	return d, nil
}

// OpenFile opens an Excel workbook from an fs.File.
func OpenFile(file fs.File) (grate.Source, error) {
	// We need to check if the file implements ReaderAt for zip.NewReader
	ra, ok := file.(io.ReaderAt)
	if !ok {
		// If not a ReaderAt, we need to read all bytes to use a bytes.Reader
		data, err := io.ReadAll(file)
		if err != nil {
			return nil, err
		}
		if closer, ok := file.(io.Closer); ok {
			closer.Close()
		}

		// Create a dummy readerat that's backed by the data
		type readAtCloser struct {
			io.ReaderAt
			io.Closer
		}
		ra = &readAtCloser{
			ReaderAt: bytes.NewReader(data),
			Closer:   io.NopCloser(nil),
		}
		file = nil // we've already closed it if possible
	}

	// Get file size
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	z, err := zip.NewReader(ra, stat.Size())
	if err != nil {
		return nil, grate.WrapErr(err, grate.ErrNotInFormat)
	}

	// Only set f to file if it's a closer, otherwise leave it nil
	var closer io.Closer
	if file != nil {
		if c, ok := file.(io.Closer); ok {
			closer = c
		}
	}

	d := &Document{
		f: closer,
		r: z,
	}

	err = d.init()
	if err != nil {
		d.Close()
		return nil, err
	}

	return d, nil
}

// OpenReader opens an Excel workbook from an io.ReadCloser.
func OpenReader(reader io.ReadCloser) (grate.Source, error) {
	// Read all data from the reader
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	
	// Close the reader since we've read all data
	if err := reader.Close(); err != nil {
		return nil, err
	}

	// Create a bytes.Reader that implements io.ReaderAt for zip.NewReader
	br := bytes.NewReader(data)
	
	// Create a zip reader
	z, err := zip.NewReader(br, int64(len(data)))
	if err != nil {
		return nil, grate.WrapErr(err, grate.ErrNotInFormat)
	}

	// Create and initialize the document
	d := &Document{
		f: nil, // We already closed the reader
		r: z,
	}

	err = d.init()
	if err != nil {
		d.Close()
		return nil, err
	}

	return d, nil
}

// init initializes the document by parsing relationships and workbook structure
func (d *Document) init() error {
	d.rels = make(map[string]map[string]string, 4)

	// parse the primary relationships
	dec, c, err := d.openXML("_rels/.rels")
	if err != nil {
		return grate.WrapErr(err, grate.ErrNotInFormat)
	}
	err = d.parseRels(dec, "")
	c.Close()
	if err != nil {
		return grate.WrapErr(err, grate.ErrNotInFormat)
	}
	if d.primaryDoc == "" {
		return errors.New("xlsx: invalid document")
	}

	// parse the secondary relationships to primary doc
	base := filepath.Base(d.primaryDoc)
	sub := strings.TrimSuffix(d.primaryDoc, base)
	relfn := fmt.Sprintf("%s%s/%s", sub, "_rels", base+".rels")
	dec, c, err = d.openXML(relfn)
	if err != nil {
		return err
	}
	err = d.parseRels(dec, sub)
	c.Close()
	if err != nil {
		return err
	}

	// parse the workbook structure
	dec, c, err = d.openXML(d.primaryDoc)
	if err != nil {
		return err
	}
	err = d.parseWorkbook(dec)
	c.Close()
	if err != nil {
		return err
	}

	styn := d.rels["http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles"]
	for _, sst := range styn {
		// parse the shared string table
		dec, c, err = d.openXML(sst)
		if err != nil {
			return err
		}
		err = d.parseStyles(dec)
		c.Close()
		if err != nil {
			return err
		}
	}

	ssn := d.rels["http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings"]
	for _, sst := range ssn {
		// parse the shared string table
		dec, c, err = d.openXML(sst)
		if err != nil {
			return err
		}
		err = d.parseSharedStrings(dec)
		c.Close()
		if err != nil {
			return err
		}
	}

	return nil
}

func (d *Document) openXML(name string) (*xml.Decoder, io.Closer, error) {
	if grate.Debug {
		log.Println("    openXML", name)
	}
	for _, zf := range d.r.File {
		if zf.Name == name {
			zfr, err := zf.Open()
			if err != nil {
				return nil, nil, err
			}
			dec := xml.NewDecoder(zfr)
			return dec, zfr, nil
		}
	}
	return nil, nil, io.EOF
}

func (d *Document) List() ([]string, error) {
	res := make([]string, 0, len(d.sheets))
	for _, s := range d.sheets {
		res = append(res, s.name)
	}
	return res, nil
}

func (d *Document) Get(sheetName string) (grate.Collection, error) {
	for _, s := range d.sheets {
		if s.name == sheetName {
			if s.err == errNotLoaded {
				s.err = s.parseSheet()
			}
			return s.wrapped, s.err
		}
	}
	return nil, errors.New("xlsx: sheet not found")
}
