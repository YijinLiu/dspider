package main

import (
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/golang/glog"
	_ "github.com/mattn/go-sqlite3"
)

const (
	STAT_COUNT       = "count"
	STAT_AVG_GOAL    = "goal"
	STAT_AVG_PLEDGED = "pledged"
	STAT_AVG_RATIO   = "ratio"
)

var dbNameFlag = flag.String("db-name", "", "")
var sqlDriverFlag = flag.String("sql-driver", "sqlite3", "")
var tableFlag = flag.String("table", "projects", "")
var columnsFlag = flag.String("columns", "name,goal,pledged,currency,usd_rate,launched_at,deadline,url,slug", "")
var groupByFlag = flag.String("group-by", "slug", "")
var outputBaseFlag = flag.String("output-base", "kickstarter", "")

func main() {
	flag.Parse()

	dbName, table, columns, groupByStr := *dbNameFlag, *tableFlag, *columnsFlag, *groupByFlag
	if dbName == "" || table == "" || columns == "" || groupByStr == "" {
		glog.Fatalf("Please specify --db-name, --table, --columns and groups.")
	}

	// Open DB.
	db, err := sql.Open(*sqlDriverFlag, dbName)
	if err != nil {
		glog.Fatalf("Failed to open '%s': %v", dbName, err)
	}
	defer db.Close()

	// Query.
	sqlStmt := fmt.Sprintf("SELECT %s from %s", columns, table)
	rows, err := db.Query(sqlStmt)
	if err != nil {
		glog.Fatalf("Failed to query '%s': %v", sqlStmt, err)
	}

	// Create column values.
	var values []interface{}
	if colTypes, err := rows.ColumnTypes(); err != nil {
		glog.Fatalf("Failed to get result column types: %v", err)
	} else {
		values = make([]interface{}, len(colTypes))
		for i, ct := range colTypes {
			switch ct.DatabaseTypeName() {
			case "TIMESTAMP":
				values[i] = &time.Time{}
			case "TEXT":
				values[i] = new(string)
			case "INTEGER":
				values[i] = new(int)
			case "REAL":
				values[i] = new(float64)
			default:
				glog.Fatalf("Don't know how to convert column '%s'(%s) to Go type",
					ct.Name(), ct.DatabaseTypeName())
			}
		}
	}

	// Get column names.
	colNames, err := rows.Columns()
	if err != nil {
		glog.Fatalf("Failed to get columns: %v", err)
	}
	groupBy := strings.Split(groupByStr, ",")
	groupIndices := make([]int, len(groupBy))
	for i, group := range groupBy {
		if j := findStr(group, colNames); j < 0 {
			glog.Fatalf("Unknown group column '%s'.", group)
		} else {
			groupIndices[i] = j
		}
	}

	outputBase := *outputBaseFlag
	groupToCsv := make(map[string]*csv.Writer)
	var groupNames []string
	startMonthToStats := make([]map[string]float64, 12)
	endMonthToStats := make([]map[string]float64, 12)
	for i := 0; i < 12; i++ {
		startMonthToStats[i] = make(map[string]float64)
		endMonthToStats[i] = make(map[string]float64)
	}

	// Iterate every row.
	var colStrs = make([]string, len(values))
	for rows.Next() {
		if err := rows.Scan(values...); err != nil {
			glog.Fatal(err)
		}
		// Convert to strings.
		var goal, pledged, usdRate float64
		var startMonth, endMonth int
		for i, val := range values {
			itfcv := reflect.Indirect(reflect.ValueOf(val)).Interface()
			if tv, ok := itfcv.(time.Time); ok {
				colStrs[i] = sqlTimeStr(tv)
			} else {
				colStrs[i] = fmt.Sprint(itfcv)
			}
			switch colNames[i] {
			case "goal":
				goal = itfcv.(float64)
			case "pledged":
				pledged = itfcv.(float64)
			case "usd_rate":
				usdRate = itfcv.(float64)
			case "launched_at":
				startMonth = int(itfcv.(time.Time).Month()) - 1
			case "deadline":
				endMonth = int(itfcv.(time.Time).Month()) - 1
			}
		}
		// Get group name.
		var groupName string
		for _, i := range groupIndices {
			if groupName != "" {
				groupName += ","
			}
			groupName += colStrs[i]
		}
		// Output to group csv.
		csvWriter := groupToCsv[groupName]
		if csvWriter == nil {
			groupOutFile := fmt.Sprintf("%s-%s.csv", outputBase, groupNameToFileName(groupName))
			if fh, err := os.Create(groupOutFile); err != nil {
				glog.Fatalf("Failed to create file '%s': %v", groupOutFile, err)
			} else {
				defer fh.Close()
				csvWriter = csv.NewWriter(fh)
				groupToCsv[groupName] = csvWriter
				if err := csvWriter.Write(colNames); err != nil {
					glog.Fatal(err)
				}
			}
			groupNames = append(groupNames, groupName)
		}
		if err := csvWriter.Write(colStrs); err != nil {
			glog.Fatal(err)
		}
		// Compute stats.
		statMap := startMonthToStats[startMonth]
		statMap[STAT_COUNT] += 1.0
		statMap[STAT_AVG_GOAL] += goal * usdRate
		statMap[STAT_AVG_PLEDGED] += pledged * usdRate
		statMap[STAT_COUNT+"-"+groupName] += 1.0
		statMap[STAT_AVG_GOAL+"-"+groupName] += goal * usdRate
		statMap[STAT_AVG_PLEDGED+"-"+groupName] += pledged * usdRate
		statMap = endMonthToStats[endMonth]
		statMap[STAT_COUNT] += 1.0
		statMap[STAT_AVG_GOAL] += goal * usdRate
		statMap[STAT_AVG_PLEDGED] += pledged * usdRate
		statMap[STAT_COUNT+"-"+groupName] += 1.0
		statMap[STAT_AVG_GOAL+"-"+groupName] += goal * usdRate
		statMap[STAT_AVG_PLEDGED+"-"+groupName] += pledged * usdRate
	}

	for _, csvWriter := range groupToCsv {
		csvWriter.Flush()
	}

	// Output stats.
	statsFH, err := os.Create(outputBase + "-stats.csv")
	if err != nil {
		glog.Fatal(err)
	}
	defer statsFH.Close()
	statsCsv := csv.NewWriter(statsFH)

	// Headers
	sort.Strings(groupNames)
	var headers []string
	headers = append(headers, "month", STAT_COUNT+"-start", STAT_AVG_GOAL+"-start",
		STAT_AVG_PLEDGED+"-start", STAT_AVG_RATIO+"-start", STAT_COUNT+"-end",
		STAT_AVG_GOAL+"-end", STAT_AVG_PLEDGED+"-end", STAT_AVG_RATIO+"-end")
	for _, groupName := range groupNames {
		headers = append(headers, STAT_COUNT+"-start-"+groupName, STAT_AVG_GOAL+"-start-"+groupName,
			STAT_AVG_PLEDGED+"-start-"+groupName, STAT_AVG_RATIO+"-start-"+groupName,
			STAT_COUNT+"-end-"+groupName, STAT_AVG_GOAL+"-end-"+groupName,
			STAT_AVG_PLEDGED+"-end-"+groupName, STAT_AVG_RATIO+"-end-"+groupName)
	}
	if err := statsCsv.Write(headers); err != nil {
		glog.Fatalf("Failed to write columns header: %v", err)
	}

	// Data
	for month := 0; month < 12; month++ {
		startStatMap := startMonthToStats[month]
		endStatMap := endMonthToStats[month]
		cols := make([]string, len(headers))
		cols[0] = fmt.Sprintf("%02d", month+1)
		count1 := startStatMap[STAT_COUNT]
		goal1 := startStatMap[STAT_AVG_GOAL]
		pledged1 := startStatMap[STAT_AVG_PLEDGED]
		cols[1] = fmt.Sprintf("%d", int(count1))
		cols[2] = fmt.Sprintf("%.1f", goal1/count1)
		cols[3] = fmt.Sprintf("%.1f", pledged1/count1)
		cols[4] = fmt.Sprintf("%.1f", pledged1/goal1)
		count2 := endStatMap[STAT_COUNT]
		goal2 := endStatMap[STAT_AVG_GOAL]
		pledged2 := endStatMap[STAT_AVG_PLEDGED]
		cols[5] = fmt.Sprintf("%d", int(count2))
		cols[6] = fmt.Sprintf("%.1f", goal2/count2)
		cols[7] = fmt.Sprintf("%.1f", pledged2/count2)
		cols[8] = fmt.Sprintf("%.1f", pledged2/goal2)
		for i, groupName := range groupNames {
			count1 := startStatMap[STAT_COUNT+"-"+groupName]
			goal1 := startStatMap[STAT_AVG_GOAL+"-"+groupName]
			pledged1 := startStatMap[STAT_AVG_PLEDGED+"-"+groupName]
			cols[9+i*8] = fmt.Sprintf("%d", int(count1))
			cols[10+i*8] = fmt.Sprintf("%.1f", goal1/count1)
			cols[11+i*8] = fmt.Sprintf("%.1f", pledged1/count1)
			cols[12+i*8] = fmt.Sprintf("%.1f", pledged1/goal1)
			count2 := endStatMap[STAT_COUNT+"-"+groupName]
			goal2 := endStatMap[STAT_AVG_GOAL+"-"+groupName]
			pledged2 := endStatMap[STAT_AVG_PLEDGED+"-"+groupName]
			cols[13+i*8] = fmt.Sprintf("%d", int(count2))
			cols[14+i*8] = fmt.Sprintf("%.1f", goal2/count2)
			cols[15+i*8] = fmt.Sprintf("%.1f", pledged2/count2)
			cols[16+i*8] = fmt.Sprintf("%.1f", pledged2/goal2)
		}
		if err := statsCsv.Write(cols); err != nil {
			glog.Fatal(err)
		}
	}
	statsCsv.Flush()
}

func sqlTimeStr(tm time.Time) string {
	return tm.Format("2006-01-02T15-04-05")
}

func findStr(str string, strs []string) int {
	for i, s := range strs {
		if s == str {
			return i
		}
	}
	return -1
}

func groupNameToFileName(name string) string {
	return strings.Replace(name, "/", "_", -1)
}
