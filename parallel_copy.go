package timescaledb_parallel_copy

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hajnalandor/timescaledb-parallel-copy/internal/db"
	"github.com/jmoiron/sqlx"
)

const (
	tabCharStr = "\\t"
)

type Opts struct {
	// Database where the destination table exists
	DBName string
	// PostgreSQL connection url (default "host=localhost user=postgres sslmode=disable")
	PostgresConnect string

	Overrides []db.Overrideable
	// Destination table's schema (default "public")
	SchemaName string
	// Destination table for insertions (default "test_table")
	TableName string
	// Truncate the destination table before insert
	Truncate bool

	// Additional options to pass to COPY (e.g., NULL 'NULL') (default "CSV")
	CopyOptions string
	// Character to split by (default ",")
	SplitCharacter string
	// File to read from
	FromFile string
	// FileReader to read from
	FileReader io.Reader
	// Comma-separated columns present in CSV
	Columns string
	// Skip the first line of the input
	SkipHeader bool
	// Number of header lines
	HeaderLinesCnt int

	TokenSize int
	// Number of parallel requests to make (default runtime.NumCPU)
	Workers int
	// Number of rows to insert overall; 0 means to insert all
	Limit int64
	// Number of rows per insert (default 5000)
	BatchSize int
	// Whether to time individual batches.
	LogBatches bool
	// Period to report insert stats; if 0s, intermediate results will not be reported
	ReportingPeriod time.Duration
	// Print more information about copying statistics
	Verbose bool
}

var rowCount int64

type batch struct {
	rows []string
}

func getFullTableName(opts Opts) string {
	return fmt.Sprintf(`"%s"."%s"`, opts.SchemaName, opts.TableName)
}

func normalizeOpts(opts *Opts) {
	if opts.PostgresConnect == "" {
		opts.PostgresConnect = "host=localhost user=postgres sslmode=disable"
	}
	if opts.TableName == "" {
		opts.TableName = "test_table"
	}
	if opts.SchemaName == "" {
		opts.SchemaName = "public"
	}
	if opts.CopyOptions == "" {
		opts.CopyOptions = "CSV"
	}
	if opts.SplitCharacter == "" {
		opts.SplitCharacter = ","
	}
	if opts.TokenSize == 0 {
		opts.TokenSize = bufio.MaxScanTokenSize
	}
	if opts.BatchSize == 0 {
		opts.BatchSize = 5000
	}
	if opts.Workers == 0 {
		opts.Workers = runtime.NumCPU()
	}

	if opts.DBName != "" {
		opts.Overrides = append(opts.Overrides, db.OverrideDBName(opts.DBName))
	}
}

func ParallelInsert(ctx context.Context, opts Opts) error {
	normalizeOpts(&opts)

	if opts.Truncate { // Remove existing data from the table
		dbx, err := db.Connect(opts.PostgresConnect, opts.Overrides...)
		if err != nil {
			return err
		}
		_, err = dbx.Exec(fmt.Sprintf("TRUNCATE %s", getFullTableName(opts)))
		if err != nil {
			return err
		}

		err = dbx.Close()
		if err != nil {
			return err
		}
	}

	var scanner *bufio.Scanner
	if len(opts.FromFile) > 0 {
		file, err := os.Open(opts.FromFile)
		if err != nil {
			return err
		}
		defer file.Close()

		scanner = bufio.NewScanner(file)
	} else if opts.FileReader != nil {
		scanner = bufio.NewScanner(opts.FileReader)
	} else {
		return errors.New("missing file name")
	}

	if opts.HeaderLinesCnt <= 0 {
		return fmt.Errorf("WARNING: provided --header-line-count (%d) must be greater than 0\n", opts.HeaderLinesCnt)
	}

	if opts.TokenSize != 0 && opts.TokenSize < bufio.MaxScanTokenSize {
		fmt.Printf("WARNING: provided --token-size (%d) is smaller than default (%d), ignoring\n", opts.TokenSize, bufio.MaxScanTokenSize)
	} else if opts.TokenSize > bufio.MaxScanTokenSize {
		buf := make([]byte, opts.TokenSize)
		scanner.Buffer(buf, opts.TokenSize)
	}

	var wg sync.WaitGroup
	batchChan := make(chan *batch, opts.Workers*2)

	// Generate COPY workers
	for i := 0; i < opts.Workers; i++ {
		wg.Add(1)
		go processBatches(opts, &wg, batchChan)
	}

	// Reporting thread
	if opts.ReportingPeriod > (0 * time.Second) {
		go report(opts)
	}

	start := time.Now()
	rowsRead := scan(opts, scanner, batchChan)
	close(batchChan)
	wg.Wait()
	end := time.Now()
	took := end.Sub(start)
	rowRate := float64(rowsRead) / float64(took.Seconds())

	res := fmt.Sprintf("COPY %d", rowsRead)
	if opts.Verbose {
		res += fmt.Sprintf(", took %v with %d worker(s) (mean rate %f/sec)", took, opts.Workers, rowRate)
	}
	fmt.Println(res)
	return nil
}

// report periodically prints the write rate in number of rows per second
func report(opts Opts) {
	start := time.Now()
	prevTime := start
	prevRowCount := int64(0)

	for now := range time.NewTicker(opts.ReportingPeriod).C {
		rCount := atomic.LoadInt64(&rowCount)

		took := now.Sub(prevTime)
		rowrate := float64(rCount-prevRowCount) / float64(took.Seconds())
		overallRowrate := float64(rCount) / float64(now.Sub(start).Seconds())
		totalTook := now.Sub(start)

		fmt.Printf("at %v, row rate %0.2f/sec (period), row rate %0.2f/sec (overall), %E total rows\n", totalTook-(totalTook%time.Second), rowrate, overallRowrate, float64(rCount))

		prevRowCount = rCount
		prevTime = now
	}

}

// scan reads lines from a bufio.Scanner, each which should be in CSV format
// with a delimiter specified by --split (comma by default)
func scan(opts Opts, scanner *bufio.Scanner, batchChan chan *batch) int64 {
	rows := make([]string, 0, opts.BatchSize)
	var linesRead int64

	if opts.SkipHeader {
		if opts.Verbose {
			fmt.Printf("Skipping the first %d lines of the input.\n", opts.HeaderLinesCnt)
		}
		for i := 0; i < opts.HeaderLinesCnt; i++ {
			scanner.Scan()
		}
	}

	for scanner.Scan() {
		if opts.Limit != 0 && linesRead >= opts.Limit {
			break
		}

		rows = append(rows, scanner.Text())
		if len(rows) >= opts.BatchSize { // dispatch to COPY worker & reset
			batchChan <- &batch{rows}
			rows = make([]string, 0, opts.BatchSize)
		}
		linesRead++
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Error reading input: %s", err.Error())
	}

	// Finished reading input, make sure last batch goes out.
	if len(rows) > 0 {
		batchChan <- &batch{rows}
	}

	return linesRead
}

// processBatches reads batches from channel c and copies them to the target
// server while tracking stats on the write.
func processBatches(opts Opts, wg *sync.WaitGroup, c chan *batch) {
	defer wg.Done()
	dbx, err := db.Connect(opts.PostgresConnect, opts.Overrides...)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer dbx.Close()

	delimStr := "'" + opts.SplitCharacter + "'"
	useSplitChar := opts.SplitCharacter
	if opts.SplitCharacter == tabCharStr {
		delimStr = "E" + delimStr
		// Need to covert the string-ified version of the character to actual
		// character for correct split
		useSplitChar = "\t"
	}

	var copyCmd string
	if opts.Columns != "" {
		copyCmd = fmt.Sprintf("COPY %s(%s) FROM STDIN WITH DELIMITER %s %s", getFullTableName(opts), opts.Columns, delimStr, opts.CopyOptions)
	} else {
		copyCmd = fmt.Sprintf("COPY %s FROM STDIN WITH DELIMITER %s %s", getFullTableName(opts), delimStr, opts.CopyOptions)
	}

	for batch := range c {
		start := time.Now()
		rows, err := processBatch(dbx, batch, copyCmd, useSplitChar)
		if err != nil {
			fmt.Println(err)
			return
		}
		atomic.AddInt64(&rowCount, rows)

		if opts.LogBatches {
			took := time.Since(start)
			fmt.Printf("[BATCH] took %v, batch size %d, row rate %f/sec\n", took, opts.BatchSize, float64(opts.BatchSize)/float64(took.Seconds()))
		}
	}
}

func processBatch(db *sqlx.DB, b *batch, copyCmd, splitChar string) (int64, error) {
	tx, err := db.Begin()
	if err != nil {
		return 0, err
	}

	stmt, err := tx.Prepare(copyCmd)
	if err != nil {
		return 0, err
	}

	for _, line := range b.rows {
		// For some reason this is only needed for tab splitting
		if splitChar == "\t" {
			sp := strings.Split(line, splitChar)
			args := make([]interface{}, len(sp))
			for i, v := range sp {
				args[i] = v
			}
			_, err = stmt.Exec(args...)
		} else {
			_, err = stmt.Exec(line)
		}

		if err != nil {
			return 0, err
		}
	}

	err = stmt.Close()
	if err != nil {
		return 0, err
	}

	err = tx.Commit()
	if err != nil {
		return 0, err
	}
	return int64(len(b.rows)), nil
}
