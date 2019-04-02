package main

import (
	"database/sql"
	"fmt"
	"github.com/MasahikoSawada/phint/pgplan"
	"github.com/golang/glog"
	_ "github.com/lib/pq"
	"github.com/urfave/cli"
	"io/ioutil"
	"os"
	"strconv"
)

const explainHeaderJson = "EXPLAIN (FORMAT JSON)"
const explainHeaderText = "EXPLAIN (FORMAT TEXT)"

func run(c *cli.Context) error {

	// Check arguments
	if c.String("command") == "" && c.String("file") == "" {
		glog.Error("Either SQL command or SQL file must be specified")
		os.Exit(1)
	}

	if c.String("type") != "json" {
		glog.Error("currently only \"json\" type is supported, sorry")
		os.Exit(1)
	}

	// Connect to PostgreSQL server
	connectStr := " host=" + c.String("host") +
		" port=" + strconv.Itoa(c.Int("port")) +
		" database=" + c.String("dbname")
	db, err := sql.Open("postgres", connectStr+" sslmode=disable")
	defer db.Close()

	if err != nil {
		glog.Fatal(err)
	}

	var sqlStr string
	if c.String("command") != "" {
		// SQL command is given
		sqlStr = c.String("command")
	} else {
		// SQL file is given, read SQl command from the file
		filename := c.String("file")
		f, err := os.Open(filename)
		defer f.Close()

		if err != nil {
			glog.Error(err)
			os.Exit(1)
		}

		b, err := ioutil.ReadAll(f)
		if err != nil {
			glog.Error(err)
			os.Exit(1)
		}
		sqlStr = string(b)
	}

	var explainHeader string
	switch c.String("type") {
	case "json":
		explainHeader = explainHeaderJson
	case "text":
		explainHeader = explainHeaderText
	default:
		glog.Errorf("unrecognized type specified: %s, ('json' and 'text' are available)", c.String("type"))
	}

	// Execute EXPLAIN command
	rows, err := db.Query(explainHeader + sqlStr)
	if err != nil {
		glog.Fatal(err)
	}

	var plan_str string

	// Get plan struct from PostgreSQL plan
	var plan *pgplan.Plan
	switch c.String("type") {
	case "json":
		rows.Next()
		rows.Scan(&plan_str)
		plan = pgplan.GetPlanFromJson(plan_str)
	case "text":
		var planStrs []string
		for rows.Next() {
			var s string
			rows.Scan(&s)
			planStrs = append(planStrs, s)
		}
		plan = pgplan.GetPlanFromText(planStrs)
	default:
		glog.Errorf("unrecognized type specified: %s, ('json' and 'text' are available)", c.String("type"))
	}

	// Get planner hint from PostgreSQL plan
	hint := pgplan.GetHintFromPlan(plan)

	// Show Hint
	fmt.Println(hint.GetAllHints())
	if c.Bool("hint-only") {
		return nil
	}

	// Show SQL
	fmt.Println("explain " + sqlStr)

	return nil
}

func main() {
	app := cli.NewApp()

	app.Name = "ppa"
	app.Version = "0.0.1"
	app.Usage = "ppa sample"

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "host",
			Value: "localhost",
			Usage: "database server host",
		},
		cli.StringFlag{
			Name:  "dbname, d",
			Value: "postgres",
			Usage: "database name to connect to",
		},
		cli.StringFlag{
			Name:  "command, c",
			Value: "",
			Usage: "sql command",
		},
		cli.StringFlag{
			Name:  "file, f",
			Value: "",
			Usage: "execute commands from file",
		},
		cli.StringFlag{
			Name:  "type, T",
			Value: "json",
			Usage: "input EXPLAIN output data types: json, text",
		},
		cli.IntFlag{
			Name:  "port, p",
			Value: 5432,
			Usage: "database server port",
		},
		cli.BoolFlag{
			Name:  "hint-only",
			Usage: "Show only SQL HINT clause",
		},
	}

	app.Action = run

	app.Run(os.Args)
}
