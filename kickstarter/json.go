package main

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/yijinliu/dspider"
)

const (
	PROJECTS_TABLE_NAME = "projects"
)

type JsonParser struct {
	wg sync.WaitGroup
}

type CreatorJson struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

type LocationJson struct {
	ID              int    `json:"id"`
	Name            string `json:"name"`
	DisplayableName string `json:"displayable_name"`
	Country         string `json:"country"`
	State           string `json:"state"`
}

type CategoryJson struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
	Slug string `json:"slug"`
}

type WebUrlsJson struct {
	Project string `json:"project"`
	Rewards string `json:"rewards"`
}

type UrlsJson struct {
	Web WebUrlsJson `json:"web"`
}

type ProjectJson struct {
	ID            int          `json:"id"`
	Name          string       `json:"name"`
	Blurb         string       `json:"blurb"`
	Goal          float64      `json:"goal"`
	Pledged       float64      `json:"pledged"`
	State         string       `json:"state"`
	Country       string       `json:"country"`
	Currency      string       `json:"currency"`
	Deadline      int64        `json:"deadline"`
	CreatedAt     int64        `json:"created_at"`
	LaunchedAt    int64        `json:"launched_at"`
	StaffPick     bool         `json:"staff_pick"`
	BackersCount  int          `json:"backers_count"`
	StaticUsdRate float64      `json:"static_usd_rate"`
	USDPledged    string       `json:"usd_pledged"`
	Creator       CreatorJson  `json:"creator"`
	Location      LocationJson `json:"location"`
	Category      CategoryJson `json:"category"`
	URLs          UrlsJson     `json:"urls"`
}

type ProjectsJson struct {
	Projects []ProjectJson `json:"projects"`
	HasMore  bool          `json:"has_more"`
}

type SQLRow struct {
	Table        string    `sql:"table"`
	ID           int       `sql:"id"`
	Name         string    `sql:"name"`
	Desc         string    `sql:"desc"`
	Goal         float64   `sql:"goal"`
	Pledged      float64   `sql:"pledged"`
	Currency     string    `sql:"currency"`
	Country      string    `sql:"country"`
	BackersCount int       `sql:"backers_count"`
	CreatedAt    time.Time `sql:"created_at"`
	LaunchedAt   time.Time `sql:"launched_at"`
	Deadline     time.Time `sql:"deadline"`
	Category     string    `sql:"category"`
	Slug         string    `sql:"slug"`
	URL          string    `sql:"url"`
}

func (p *JsonParser) Parse(urlStr string, resp *http.Response, spider dspider.Spider) error {
	if resp == nil || resp.StatusCode != http.StatusOK {
		p.wg.Done()
		return nil
	}
	var projects ProjectsJson
	if body, err := ioutil.ReadAll(resp.Body); err != nil {
		return err
	} else if err := json.Unmarshal(body, &projects); err != nil {
		return err
	}
	for _, project := range projects.Projects {
		if project.State == "live" {
			continue
		}
		glog.V(2).Infof("Adding project %d/%s ...", project.ID, project.Name)
		row := &SQLRow{
			Table:        PROJECTS_TABLE_NAME,
			ID:           project.ID,
			Name:         project.Name,
			Desc:         project.Blurb,
			Goal:         project.Goal,
			Pledged:      project.Pledged,
			Currency:     project.Currency,
			Country:      project.Country,
			BackersCount: project.BackersCount,
			CreatedAt:    time.Unix(project.CreatedAt, 0),
			LaunchedAt:   time.Unix(project.LaunchedAt, 0),
			Deadline:     time.Unix(project.Deadline, 0),
			Category:     project.Category.Name,
			Slug:         project.Category.Slug,
			URL:          project.URLs.Web.Project,
		}
		if err := spider.AddDoc(row.URL, row); err != nil {
			glog.Warningf("Failed to add '%s': %v", row.URL, err)
		}
	}

	if projects.HasMore {
		// Crawl next page.
		urlObj, _ := url.Parse(urlStr)
		q := urlObj.Query()
		page := q.Get("page")
		if page == "" {
			q.Set("page", "2")
		} else if ipage, err := strconv.Atoi(page); err != nil {
			glog.V(0).Infof("Failed to parse page paramer: %v", err)
			q.Set("page", "2")
		} else {
			q.Set("page", strconv.Itoa(ipage+1))
		}
		urlObj.RawQuery = q.Encode()
		go spider.Queue(urlObj.String())
	} else {
		p.wg.Done()
	}

	return nil
}
