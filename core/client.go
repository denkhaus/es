package core

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/olivere/elastic/v7"
	"github.com/pkg/errors"
)

var (
	ErrNotAcknowledged = errors.Errorf("not acknowledged")
	ErrToManyHits      = errors.Errorf("to many hits")
	ErrEmptyInput      = errors.Errorf("empty input")
	ErrEmptyResult     = errors.Errorf("empty result")
	ErrEmptyResponse   = errors.Errorf("empty response")
)

type IDProvider interface {
	ID() string
}

type IndexNameProvider interface {
	IndexName() string
}

type IndexNameAndIDProvider interface {
	IDProvider
	IndexNameProvider
}

//get index list from ES and parse indices from it
//return a map where every prefix from input array is a key
//and a value is vector of corresponding indices
func (c *ElasticClientImpl) GetIndices(prefixes []string) (map[string][]string, error) {
	result := make(map[string][]string)

	req, err := http.NewRequest("GET", c.endpoint+"/_cat/indices?v&s=index", nil)
	if err != nil {
		return nil, errors.Wrap(err, "NewRequest")
	}

	req.SetBasicAuth(c.userName, c.password)

	client := http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return result, nil
	}

	defer resp.Body.Close()
	scanner := bufio.NewScanner(resp.Body)
	scanner.Split(bufio.ScanLines)

	var lines []string
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}

	for _, prefix := range prefixes {
		r, err := regexp.Compile("\\s" + prefix + "-(\\d)*\\s")
		if err != nil {
			return result, nil
		}
		for _, line := range lines {
			match := r.FindString(line)
			if len(match) != 0 {
				result[prefix] = append(result[prefix], strings.TrimSpace(match))
			}
		}
	}

	return result, nil
}

func (c *ElasticClientImpl) Search(ctx context.Context, p *SearchParameters) (
	*elastic.SearchResult, error) {

	searchService := c.client.Search(p.Index).
		Query(p.Query).
		From(p.From).
		SortBy(p.Sorter...)

	if p.PageSize != 0 {
		searchService.Size(p.PageSize)
	}

	if len(p.SearchAfter) != 0 {
		searchService.SearchAfter(p.SearchAfter...)
	}

	return searchService.Do(ctx)
}

func (c *ElasticClientImpl) MarshalWithNameAndIDProvider(ctx context.Context, data IndexNameAndIDProvider) error {
	service := c.client.Index().Index(data.IndexName()).Id(data.ID())
	if _, err := service.BodyJson(data).Do(ctx); err != nil {
		return errors.Wrap(err, "Do [create")
	}

	return nil
}

func (c *ElasticClientImpl) UnmarshalOne(

	ctx context.Context,
	indexName string,
	query elastic.Query,
	target interface{},

) error {

	svc := c.client.
		Search(indexName).
		Query(query).
		Size(1)

	res, err := svc.Do(ctx)
	if err != nil {
		return errors.Wrap(err, "Do")
	}

	if res.TotalHits() == 0 {
		return ErrEmptyResult
	}

	if err := json.Unmarshal(res.Hits.Hits[0].Source, target); err != nil {
		return errors.Wrap(err, "Unmarshal")
	}

	return nil
}

func (c *ElasticClientImpl) UnmarshalMostRecent(

	ctx context.Context,
	indexName string,
	query elastic.Query,
	timestampField string,
	target interface{},

) error {

	sorter := elastic.NewFieldSort(timestampField).Desc()

	svc := c.client.
		Search(indexName).
		Query(query).
		Size(1).
		SortBy(sorter)

	res, err := svc.Do(ctx)
	if err != nil {
		return errors.Wrap(err, "Do")
	}

	if res.TotalHits() == 0 {
		return ErrEmptyResult
	}

	if err := json.Unmarshal(res.Hits.Hits[0].Source, target); err != nil {
		return errors.Wrap(err, "Unmarshal")
	}

	return nil
}

func (c *ElasticClientImpl) SearchWithDSL(ctx context.Context, index, query string) (
	*elastic.SearchResult, error) {
	return c.client.Search(index).Source(query).Do(ctx)
}

func (c *ElasticClientImpl) ScrollService(
	index string,
	query elastic.Query,
	sorter elastic.Sorter,
) *elastic.ScrollService {

	svc := elastic.NewScrollService(c.client)
	svc = svc.Index(index).Query(query)
	if sorter != nil {
		svc.SortBy(sorter)
	}

	return svc
}

func (c *ElasticClientImpl) ClearScroll(ctx context.Context, scrollID string) error {
	if scrollID == "" {
		return nil
	}

	if _, err := c.client.ClearScroll(scrollID).Do(ctx); err != nil {
		return err
	}

	return nil
}

func (c *ElasticClientImpl) EnumerateItems(
	ctx context.Context,
	indexName string,
	query elastic.Query,
	sorter elastic.Sorter,
	onItem func(item json.RawMessage, nCurrentItem, nTotalItems int64, commit bool) error,
) error {

	errs := new(multierror.Error)
	svc := c.ScrollService(indexName, query, sorter)
	defer svc.Clear(ctx)

	var nCurrentItem int64
	var scrollID string

	for len(errs.Errors) == 0 {
		if scrollID != "" {
			svc = svc.ScrollId(scrollID)
		}

		res, err := svc.Do(ctx)
		if err != nil {
			if err != io.EOF {
				errs = multierror.Append(errs, errors.Wrap(err, "Do"))
			}
			break
		}

		hits := res.Hits.Hits
		nBatchItems := len(hits)
		nTotalItems := res.Hits.TotalHits.Value
		scrollID = res.ScrollId

		for idx, hit := range hits {
			nCurrentItem++
			commit := idx == nBatchItems-1
			if err := onItem(hit.Source, nCurrentItem, nTotalItems, commit); err != nil {
				errs = multierror.Append(errs, errors.Wrap(err, "onItem"))
				break
			}
		}

		select {
		default:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if err := c.ClearScroll(ctx, scrollID); err != nil {
		errs = multierror.Append(errs, errors.Wrap(err, "ClearScroll"))
	}

	return errs.ErrorOrNil()
}

func (c *ElasticClientImpl) Count(ctx context.Context, index, query string) (int64, error) {
	return c.client.Count(index).BodyString(query).Do(ctx)
}

func (c *ElasticClientImpl) FlushIndex(ctx context.Context, index string) error {
	_, err := c.client.Flush(index).Do(ctx)

	if err != nil {
		return errors.Wrap(err, "Flush")
	}

	return nil
}

func (c *ElasticClientImpl) EnsureIndexWithMapping(ctx context.Context, indexName, mapping string) error {
	exists, err := c.client.IndexExists(indexName).Do(ctx)
	if err != nil {
		return errors.Wrap(err, "IndexExists")
	}

	if !exists {
		createIndex, err := c.client.CreateIndex(indexName).BodyString(mapping).Do(ctx)

		if err != nil {
			return errors.Wrap(err, "CreateIndex")
		}

		if !createIndex.Acknowledged {
			return ErrNotAcknowledged
		}
	}

	return nil
}

func (c *ElasticClientImpl) DoIndex(ctx context.Context, indexName string, data map[string]interface{}) error {
	service := c.client.Index().Index(indexName)
	for idx, dat := range data {
		_, err := service.Id(idx).BodyJson(&dat).Do(ctx)
		if err != nil {
			return errors.Wrap(err, "Do [create")
		}
	}

	return nil
}

func (c *ElasticClientImpl) DoIndexWithNameProvider(ctx context.Context, data map[string]IndexNameProvider) error {
	service := c.client.Index()
	for idx, dat := range data {
		_, err := service.Id(idx).Index(dat.IndexName()).BodyJson(&dat).Do(ctx)
		if err != nil {
			return errors.Wrap(err, "Do [create")
		}
	}

	return nil
}

func (c *ElasticClientImpl) DoCreate(ctx context.Context, indexName string, data map[string]interface{}) error {
	service := c.client.Index().Index(indexName).OpType("create")
	for idx, dat := range data {
		_, err := service.Id(idx).BodyJson(&dat).Do(ctx)
		if err != nil {
			return errors.Wrap(err, "Do [index]")
		}
	}

	return nil
}

func (c *ElasticClientImpl) RunBulkProcessor(ctx context.Context, p *BulkProcessorParameters) (
	*elastic.BulkProcessor, error) {

	return c.client.BulkProcessor().
		Name(p.Name).
		Workers(p.NumOfWorkers).
		BulkActions(p.BulkActions).
		BulkSize(p.BulkSize).
		FlushInterval(p.FlushInterval).
		Backoff(p.Backoff).
		Before(p.BeforeFunc).
		After(p.AfterFunc).
		Do(ctx)
}

// root is for nested object like Attr property for search attributes.
func (c *ElasticClientImpl) PutMapping(ctx context.Context, index, root, key, valueType string) error {
	body := buildPutMappingBody(root, key, valueType)
	_, err := c.client.PutMapping().Index(index).BodyJson(body).Do(ctx)
	return err
}

func (c *ElasticClientImpl) CreateIndex(ctx context.Context, index string) error {
	_, err := c.client.CreateIndex(index).Do(ctx)
	return err
}

func (c *ElasticClientImpl) Ping() *elastic.PingService {
	return c.client.Ping(c.endpoint)
}

func buildPutMappingBody(root, key, valueType string) map[string]interface{} {
	body := make(map[string]interface{})
	if len(root) != 0 {
		body["properties"] = map[string]interface{}{
			root: map[string]interface{}{
				"properties": map[string]interface{}{
					key: map[string]interface{}{
						"type": valueType,
					},
				},
			},
		}
	} else {
		body["properties"] = map[string]interface{}{
			key: map[string]interface{}{
				"type": valueType,
			},
		}
	}
	return body
}

func NewClient(endpoint, userName, password string, healthCheckInterval time.Duration, sniff bool) (*ElasticClientImpl, error) {
	client, err := elastic.NewClient(
		elastic.SetSniff(sniff),
		elastic.SetURL(endpoint),
		elastic.SetHealthcheckInterval(healthCheckInterval),
		elastic.SetBasicAuth(userName, password),
		elastic.SetRetrier(elastic.NewBackoffRetrier(
			elastic.NewExponentialBackoff(128*time.Millisecond, 513*time.Millisecond)),
		),
		// critical to ensure decode of int64 won't lose precision
		elastic.SetDecoder(&elastic.NumberDecoder{}),
	)

	if err != nil {
		return nil, errors.Wrap(err, "NewClient")
	}

	return &ElasticClientImpl{
		endpoint: endpoint,
		userName: userName,
		password: password,
		client:   client,
	}, nil
}
