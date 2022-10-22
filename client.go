package es

import (
	"context"
	"time"

	"github.com/denkhaus/es/core"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

var (
	clientInstance *core.ElasticClient
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

func Get(ctx context.Context, endpoint, userName, password string, healthCheckInterval time.Duration) (*core.ElasticClient, error) {
	if clientInstance == nil {
		client, err := core.NewClient(
			endpoint,
			userName,
			password,
			healthCheckInterval,
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
