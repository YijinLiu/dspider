package dspider

import (
	"net/http"
)

type DocParser interface {
	Parse(urlStr string, resp *http.Response, spider Spider) error
}
