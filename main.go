package main

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
)

const (
	flagRowCount    = "rows"
	flagChunkRows   = "chk_rows"
	flagRegionCount = "regions"
	flagSkewed      = "skewed"
	flagDumplingBin = "dumpling"
	flagAction      = "action"
)

var (
	rowCount      int
	chunkRowCount int
	regionCount   int
	skewed        bool
	dumplingBin   string
	action        string
)

func parseFlags(flags *pflag.FlagSet) (err error) {
	flags.Int(flagRowCount, 100000, "Number of rows to generate in a table, default 100000")
	flags.Int(flagChunkRows, 10000, "Number of rows to split chunk, default 100000")
	flags.Int(flagRegionCount, 16, "Number of regions of the table, default 16")
	flags.Bool(flagSkewed, false, "Whether the data is heavily skewed, default false")
	flags.String(flagDumplingBin, "./dumpling", "The binary of dumpling, default ./dumpling")
	flags.String(flagAction, "all", "{prepare|run|all}, default all")
	flags.Bool("help", false, "Print help message and quit")
	pflag.Parse()
	if printHelp, err := pflag.CommandLine.GetBool("help"); printHelp || err != nil {
		if err != nil {
			fmt.Printf("\nGet help flag error: %s\n", err)
		}
		pflag.Usage()
		os.Exit(0)
	}
	rowCount, err = flags.GetInt(flagRowCount)
	if err != nil {
		return err
	}
	chunkRowCount, err = flags.GetInt(flagChunkRows)
	if err != nil {
		return err
	}
	regionCount, err = flags.GetInt(flagRegionCount)
	if err != nil {
		return err
	}
	skewed, err = flags.GetBool(flagSkewed)
	if err != nil {
		return err
	}
	dumplingBin, err = flags.GetString(flagDumplingBin)
	if err != nil {
		return err
	}
	action, err = flags.GetString(flagAction)
	if err != nil {
		return err
	}
	switch action {
	case "all", "prepare", "run":
	default:
		return errors.Errorf("unknown action: %s", action)
	}
	return nil
}

func main() {
	pflag.Usage = func() {
		fmt.Fprint(os.Stderr, "Dumpling_bench is a CLI tool that helps you bench Dumpling.\n\nUsage:\n  dumpling_bench [flags]\n\nFlags:\n")
		pflag.PrintDefaults()
	}
	err := parseFlags(pflag.CommandLine)
	if err != nil {
		fmt.Printf("\nparse arguments failed: %+v\n", err)
		os.Exit(1)
	}
	if pflag.NArg() > 0 {
		fmt.Printf("\nmeet some unparsed arguments, please check again: %+v\n", pflag.Args())
		os.Exit(1)
	}

	ctx := context.Background()
	pool, err := sql.Open("mysql", getDSN("root", "", "127.0.0.1", 4000))
	if err != nil {
		log.Fatalf("%v\n", err)
	}
	conn, err := pool.Conn(ctx)
	if err != nil {
		log.Fatalf("%v\n", err)
	}
	err = runSQL(ctx, conn, "use test")
	if err != nil {
		log.Fatalf("%v\n", err)
	}
	if action == "all" || action == "prepare" {
		err = prepareData(ctx, conn)
		if err != nil {
			log.Fatalf("%v\n", err)
		}
	}
	if action == "all" || action == "run" {
		err = dumpData(ctx)
		if err != nil {
			log.Fatalf("%v\n", err)
		}
	}
}

func getDSN(user, pass, host string, port int) string {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&readTimeout=%s&writeTimeout=30s&interpolateParams=true&maxAllowedPacket=0",
		user, pass, host, port, "test", "900s")
	return dsn
}

func prepareData(ctx context.Context, conn *sql.Conn) error {
	dropSQL := "drop table if exists t;"
	err := runSQL(ctx, conn, dropSQL)
	if err != nil {
		return err
	}
	createSQL := "create table t (a bigint primary key auto_increment, b int, c int, d varchar(255));"
	err = runSQL(ctx, conn, createSQL)
	if err != nil {
		return err
	}
	if regionCount != 0 {
		query := fmt.Sprintf("split table t between (0) and (%d) regions %d", rowCount, regionCount)
		err := runSQL(ctx, conn, query)
		if err != nil {
			return err
		}
	}
	var sb strings.Builder
	for i := 0; i < rowCount; i++ {
		if sb.Len() > 1000000 {
			err := runSQL(ctx, conn, fmt.Sprintf("insert into t values %s;", sb.String()))
			if err != nil {
				return err
			}
			sb.Reset()
		}
		if sb.Len() > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(fmt.Sprintf("(%d, %d, %d, '%s')", i+1, i+1, i+1, "string_payload_payload_payload"))
	}
	if sb.Len() > 0 {
		err := runSQL(ctx, conn, fmt.Sprintf("insert into t values %s;", sb.String()))
		if err != nil {
			return err
		}
	}
	if skewed {
		query := fmt.Sprintf("insert into t values (%d, %d, %d, '%s');", 9223372035854775807, rowCount+1, rowCount+1, "string_payload_payload_payload")
		err := runSQL(ctx, conn, query)
		if err != nil {
			return err
		}
	}
	return nil
}

func dumpData(ctx context.Context) error {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	start := time.Now()
	args := []string{
		"--host", "127.0.0.1",
		"--port", "4000",
		"--filter", "test.t",
		"--tidb-mem-quota-query", "8589934592", /* 8 << 20 */
		"--logfile", "dump.log",
		"--rows", fmt.Sprintf("%d", chunkRowCount),
		"--loglevel", "debug",
		"--threads", "32",
	}
	cmd := exec.Command(dumplingBin, args...)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		log.Println(stderr.String())
		return err
	}
	log.Println(stdout.String())
	elapsed := time.Since(start)
	log.Printf("dumpling took %s", elapsed)
	return nil
}

func runSQL(ctx context.Context, conn *sql.Conn, query string) error {
	if len(query) > 30 {
		log.Println(query[:30] + "...")
	} else {
		log.Println(query)
	}

	_, err := conn.ExecContext(ctx, query)
	if err != nil {
		return err
	}
	return nil
}
