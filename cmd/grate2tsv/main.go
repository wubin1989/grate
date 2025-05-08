// Command grate2tsv is a highly parallel tabular data extraction tool. It's
// probably not necessary in your situation, but is included here since it
// is a good stress test of the codebase.
//
// Files on the command line will be parsed and extracted to the "results"
// subdirectory under a heirarchical arrangement (to make our filesystems
// more responsive), and a "results.txt" file will be created logging basic
// information and errors for each file.
package main

import (
	"bufio"
	"crypto/md5"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"time"

	"github.com/wubin1989/grate"
	_ "github.com/wubin1989/grate/simple"
	_ "github.com/wubin1989/grate/xls"
	_ "github.com/wubin1989/grate/xlsx"
)

var (
	logfile        = flag.String("l", "", "save processing logs to `filename.txt`")
	pretend        = flag.Bool("p", false, "pretend to output .tsv")
	infoFile       = flag.String("i", "results.txt", "`filename` to record stats about the process")
	removeNewlines = flag.Bool("r", true, "remove embedded tabs, newlines, and condense spaces in cell contents")
	trimSpaces     = flag.Bool("w", true, "trim whitespace from cell contents")
	skipBlanks     = flag.Bool("b", true, "discard blank rows from the output")
	cpuprofile     = flag.String("cpuprofile", "", "write cpu profile to file")
	memprofile     = flag.String("memprofile", "", "write memory profile to file")

	timeFormat = "2006-01-02 15:04:05"
	fstats     *os.File

	procWG  sync.WaitGroup
	cleanup = make(chan *output, 100)
	outpool = sync.Pool{New: func() interface{} {
		return &output{}
	}}
)

type output struct {
	f *os.File
	b *bufio.Writer
}

func main() {
	flag.Parse()

	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal(err)
		}
		defer func() {
			runtime.GC()
			pprof.WriteHeapProfile(f)
			f.Close()
		}()
	}

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	if *logfile != "" {
		fo, err := os.Create(*logfile)
		if err != nil {
			log.Fatal(err)
		}
		defer fo.Close()
		log.SetOutput(fo)
	}

	done := make(chan int)
	go func() {
		for x := range cleanup {
			x.b.Flush()
			x.f.Close()
			outpool.Put(x)
		}
		done <- 1
	}()

	var err error
	fstats, err = os.OpenFile(*infoFile, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer fstats.Close()
	pos, err := fstats.Seek(0, io.SeekEnd)
	if err != nil {
		log.Fatal(err)
	}
	if pos == 0 {
		fmt.Fprintf(fstats, "time\tfilename\tsheet\trows\tcolumns\terrors\n")
	}

	filenameChan := make(chan string)

	// fan out to 1/2 of CPU cores
	// (e.g. each file-processor can use 2 cpus)
	outMu := &sync.Mutex{}
	nparallel := runtime.NumCPU() / 2
	procWG.Add(nparallel)
	for i := 0; i < nparallel; i++ {
		go runProcessor(filenameChan, outMu)
	}
	for _, fn := range flag.Args() {
		filenameChan <- fn
	}

	close(filenameChan)
	procWG.Wait()
	close(cleanup)
	<-done
}

func runProcessor(from chan string, mu *sync.Mutex) {
	for fn := range from {
		nowFmt := time.Now().Format(timeFormat)
		results, err := processFile(fn)
		mu.Lock()
		if err != nil {
			// returned errors are fatal
			fmt.Fprintf(fstats, "%s\t%s\t-\t-\t-\t%s\n", nowFmt, fn, err.Error())
			mu.Unlock()
			continue
		}

		for _, res := range results {
			e := "-"
			if res.Err != nil {
				e = res.Err.Error()
			}
			fmt.Fprintf(fstats, "%s\t%s\t%s\t%d\t%d\t%s\n", nowFmt, res.Filename, res.SheetName,
				res.NumRows, res.NumCols, e)
		}
		mu.Unlock()
	}
	procWG.Done()
}

var (
	sanitize = regexp.MustCompile("[^a-zA-Z0-9]+")
	newlines = regexp.MustCompile("[ \n\r\t]+")
)

type stats struct {
	Filename  string
	Hash      string
	SheetName string
	NumRows   int
	NumCols   int
	Err       error
}

func processFile(fn string) ([]stats, error) {
	//log.Printf("Opening file '%s' ...", fn)
	wb, err := grate.Open(fn)
	if err != nil {
		return nil, err
	}
	defer wb.Close()

	results := []stats{}

	ext := filepath.Ext(fn)
	fn2 := filepath.Base(strings.TrimSuffix(fn, ext))
	subparts := fmt.Sprintf("%x", md5.Sum([]byte(fn2)))
	subdir := filepath.Join("results", subparts[:2], subparts[2:4])
	os.MkdirAll(subdir, 0755)
	log.Printf(subparts[:8]+"  Processing file '%s'", fn2)

	sheets, err := wb.List()
	if err != nil {
		return nil, err
	}
	for _, s := range sheets {
		ps := stats{
			Filename:  fn,
			Hash:      subparts[:8],
			SheetName: s,
		}
		log.Printf(subparts[:8]+"  Opening Sheet '%s'...", s)
		sheet, err := wb.Get(s)
		if err != nil {
			ps.Err = err
			results = append(results, ps)
			continue
		}
		if sheet.IsEmpty() {
			log.Println(subparts[:8] + "    Empty sheet. Skipping.")
			results = append(results, ps)
			continue
		}
		s2 := sanitize.ReplaceAllString(s, "_")
		if s == fn {
			s2 = "main"
		}
		var ox *output
		var w io.Writer = ioutil.Discard
		if !*pretend {
			f, err := os.Create(subdir + "/" + fn2 + "." + s2 + ".tsv")
			if err != nil {
				return nil, err
			}
			ox = outpool.Get().(*output)
			ox.f = f
			ox.b = bufio.NewWriter(f)
			w = ox.b
		}

		for sheet.Next() {
			row := sheet.Strings()
			nonblank := false
			for i, x := range row {
				if *removeNewlines {
					x = newlines.ReplaceAllString(x, " ")
				}
				if *trimSpaces {
					x = strings.TrimSpace(x)
					row[i] = x
				}
				if x != "" {
					nonblank = true
					if ps.NumCols < i {
						ps.NumCols = i
					}
				}
			}
			if nonblank || !*skipBlanks {
				for i, v := range row {
					if i != 0 {
						w.Write([]byte{'\t'})
					}
					w.Write([]byte(v))
				}
				w.Write([]byte{'\n'})
				ps.NumRows++
			}
		}
		results = append(results, ps)
		if ox != nil {
			cleanup <- ox
		}
	}
	return results, nil
}
