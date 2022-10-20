package core

import (
	"time"

	"github.com/olivere/elastic/v7"
)

type (

	// elasticWrapper implements Client
	ElasticClient struct {
		client   *elastic.Client
		endpoint string
		userName string
		password string
	}

	// SearchParameters holds all required and optional parameters for executing a search
	SearchParameters struct {
		Index       string
		Query       elastic.Query
		From        int
		PageSize    int
		Sorter      []elastic.Sorter
		SearchAfter []interface{}
	}

	// BulkProcessorParameters holds all required and optional parameters for executing bulk service
	BulkProcessorParameters struct {
		Name          string
		NumOfWorkers  int
		BulkActions   int
		BulkSize      int
		FlushInterval time.Duration
		Backoff       elastic.Backoff
		BeforeFunc    elastic.BulkBeforeFunc
		AfterFunc     elastic.BulkAfterFunc
	}
)
