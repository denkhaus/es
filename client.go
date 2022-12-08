package es

import (
	"context"
	"encoding/json"
	"time"

	"github.com/denkhaus/es/core"
	"github.com/olivere/elastic/v7"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

var (
	clientInstance *core.ElasticClientImpl
)

// func ensureIndices(ctx context.Context, client *core.ElasticClient) error {
// 	if err := client.EnsureIndexWithMapping(
// 		ctx, defaults.ElasticSearchTransfersIndexName,
// 		models.ESMappingTransfer,
// 	); err != nil {
// 		return errors.Wrap(err, "ensureIndex [transfer]")
// 	}

// 	if err := client.EnsureIndexWithMapping(
// 		ctx, defaults.ElasticSearchBlocksIndexName,
// 		models.ESMappingBlock,
// 	); err != nil {
// 		return errors.Wrap(err, "EnsureIndexWithMapping [block]")
// 	}

// 	return nil
// }

type ElasticClient interface {
	ClearScroll(ctx context.Context, scrollID string) error
	Count(ctx context.Context, index string, query string) (int64, error)
	CreateIndex(ctx context.Context, index string) error
	DoCreate(ctx context.Context, indexName string, data map[string]interface{}) error
	DoIndex(ctx context.Context, indexName string, data map[string]interface{}) error
	DoIndexWithNameProvider(ctx context.Context, data map[string]core.IndexNameProvider) error
	EnsureIndexWithMapping(ctx context.Context, indexName string, mapping string) error
	EnumerateItems(ctx context.Context, indexName string, query elastic.Query, sorter elastic.Sorter, onItem func(item json.RawMessage, nCurrentItem int64, nTotalItems int64, commit bool) error) error
	FlushIndex(ctx context.Context, index string) error
	GetIndices(prefixes []string) (map[string][]string, error)
	MarshalWithNameAndIDProvider(ctx context.Context, data core.IndexNameAndIDProvider) error
	Ping() *elastic.PingService
	PutMapping(ctx context.Context, index string, root string, key string, valueType string) error
	RunBulkProcessor(ctx context.Context, p *core.BulkProcessorParameters) (*elastic.BulkProcessor, error)
	ScrollService(index string, query elastic.Query, sorter elastic.Sorter) *elastic.ScrollService
	Search(ctx context.Context, p *core.SearchParameters) (*elastic.SearchResult, error)
	SearchWithDSL(ctx context.Context, index string, query string) (*elastic.SearchResult, error)
	UnmarshalMostRecent(ctx context.Context, indexName string, query elastic.Query, timestampField string, target interface{}) error
	UnmarshalOne(ctx context.Context, indexName string, query elastic.Query, target interface{}) error
}

func Get(ctx context.Context, endpoint, userName, password string, healthCheckInterval time.Duration, sniff bool) (ElasticClient, error) {
	if clientInstance == nil {
		client, err := core.NewClient(
			endpoint,
			userName,
			password,
			healthCheckInterval,
			sniff,
		)

		if err != nil {
			return nil, errors.Wrap(err, "NewClient")
		}

		info, code, err := client.Ping().Do(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "Ping")
		}

		zlog.Debug("elasticsearch client created",
			zap.Int("code", code),
			zap.String("version", info.Version.Number),
		)

		clientInstance = client
	}

	return clientInstance, nil
}
