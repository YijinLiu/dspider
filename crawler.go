package dspider

import (
	"net/http"
)

type Crawler interface {
	Crawl(client *http.Client, urlStr string) (*http.Response, error)
}
