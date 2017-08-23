package dspider

import (
	"fmt"
	"net/http"
	"regexp"
	"sync"

	"github.com/golang/glog"
)

type Spider interface {
	Queue(urlStr string)
	Shutdown()
	// For DocParser. Users should not call this method.
	AddDoc(urlStr string, doc interface{}) error
}

type docParserSpec struct {
	regex *regexp.Regexp
	dp    DocParser
}

type storageSpec struct {
	regex *regexp.Regexp
	s     Storage
}

type crawlerSpec struct {
	regex *regexp.Regexp
	c     Crawler
}

type SimpleSpider struct {
	client   *http.Client
	parsers  []docParserSpec
	storages []storageSpec
	crawlers []crawlerSpec
	queue    chan string
	retrier  Retrier
	wg       sync.WaitGroup
}

func NewSimpleSpider(client *http.Client, maxCrawls int, retrier Retrier) *SimpleSpider {
	s := &SimpleSpider{
		client:  client,
		queue:   make(chan string),
		retrier: retrier,
	}
	s.wg.Add(maxCrawls)
	for i := 0; i < maxCrawls; i++ {
		go s.crawlLoop()
	}
	return s
}

func (s *SimpleSpider) AddDocParser(regex string, dp DocParser) {
	s.parsers = append(s.parsers, docParserSpec{
		regex: regexp.MustCompile(regex),
		dp:    dp,
	})
}

func (s *SimpleSpider) AddStorage(regex string, storage Storage) {
	s.storages = append(s.storages, storageSpec{
		regex: regexp.MustCompile(regex),
		s:     storage,
	})
}

func (s *SimpleSpider) AddCrawler(regex string, c Crawler) {
	s.crawlers = append(s.crawlers, crawlerSpec{
		regex: regexp.MustCompile(regex),
		c:     c,
	})
}

func (s *SimpleSpider) Queue(urlStr string) {
	s.queue <- urlStr
}

func (s *SimpleSpider) Shutdown() {
	close(s.queue)
	s.wg.Wait()
}

func (s *SimpleSpider) AddDoc(urlStr string, doc interface{}) error {
	for _, spec := range s.storages {
		if spec.regex.MatchString(urlStr) {
			return spec.s.AddDoc(doc)
		}
	}
	return fmt.Errorf("no storage specified for '%s'", urlStr)
}

func (s *SimpleSpider) crawlLoop() {
	defer s.wg.Done()
	for urlStr := range s.queue {
		if dp := s.docParser(urlStr); dp != nil {
			glog.V(1).Infof("Crawling '%s' ...", urlStr)
			if resp, err := s.crawl(urlStr); err == nil {
				if err = dp.Parse(urlStr, resp, s); err != nil {
					glog.V(0).Infof("Failed to parse '%s': %v", urlStr, err)
				}
				resp.Body.Close()
			} else {
				glog.V(0).Infof("Failed to crawl '%s': %v", urlStr, err)
				dp.Parse(urlStr, nil, s)
			}
		}
	}
}

func (s *SimpleSpider) docParser(urlStr string) DocParser {
	for _, spec := range s.parsers {
		if spec.regex.MatchString(urlStr) {
			return spec.dp
		}
	}
	return nil
}

func (s *SimpleSpider) crawl(urlStr string) (resp *http.Response, err error) {
	crawler := s.client.Get
	for _, spec := range s.crawlers {
		if spec.regex.MatchString(urlStr) {
			crawler = func(urlStr string) (*http.Response, error) {
				return spec.c.Crawl(s.client, urlStr)
			}
			break
		}
	}
	s.retrier.RunWithRetry(func() error {
		resp, err = crawler(urlStr)
		return err
	})
	return
}
