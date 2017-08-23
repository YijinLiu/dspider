package main

import (
	"flag"
	"fmt"
	"net/http"
	"time"

	"github.com/golang/glog"
	_ "github.com/mattn/go-sqlite3"
	"github.com/yijinliu/dspider"
)

var maxConcurrentCrawlsFlag = flag.Int("max-concurrent-crawls", 2, "")
var maxCrawlRetriesFlag = flag.Int("max-crawl-retries", 3, "")
var crawlRetryIntervalFlag = flag.Duration("crawl-retry-interval", 3*time.Second, "")
var outputFileFlag = flag.String("output-file", "", "")

func main() {
	flag.Parse()

	spider := dspider.NewSimpleSpider(http.DefaultClient, *maxConcurrentCrawlsFlag,
		&dspider.SimpleRetrier{
			Times:    *maxCrawlRetriesFlag,
			Interval: *crawlRetryIntervalFlag,
		})
	var parser JsonParser
	spider.AddDocParser("^http://www[.]kickstarter[.]com/discover/categories/", &parser)
	outputFile := *outputFileFlag
	if outputFile == "" {
		outputFile = fmt.Sprintf("kickstarter-%s.sqlite3", time.Now().Format("20060102"))
	}
	storage, err := dspider.NewSqlStorage("sqlite3", outputFile, []dspider.SqlTableDef{
		dspider.SqlTableDef{
			Name: PROJECTS_TABLE_NAME,
			Columns: map[string]string{
				"id":            "INTEGER PRIMARY KEY",
				"name":          "TEXT NOT NULL",
				"desc":          "TEXT",
				"goal":          "REAL NOT NULL",
				"pledged":       "REAL NOT NULL",
				"currency":      "TEXT NOT NULL",
				"country":       "TEXT NOT NULL",
				"backers_count": "INTEGER",
				"created_at":    "TIMESTAMP NOT NULL",
				"launched_at":   "TIMESTAMP NOT NULL",
				"deadline":      "TIMESTAMP NOT NULL",
				"category":      "TEXT",
				"slug":          "TEXT",
				"url":           "TEXT",
			},
		},
	})
	if err != nil {
		glog.Fatal(err)
	}
	defer storage.Close()
	spider.AddStorage("^https://www[.]kickstarter[.]com/projects/", storage)
	parser.wg.Add(3)
	spider.Queue("http://www.kickstarter.com/discover/categories/technology?format=json&sort=end_date")
	spider.Queue("http://www.kickstarter.com/discover/categories/crafts?format=json&sort=end_date")
	spider.Queue("http://www.kickstarter.com/discover/categories/design?format=json&sort=end_date")
	parser.wg.Wait()
	spider.Shutdown()
}
