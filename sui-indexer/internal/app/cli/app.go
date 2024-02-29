package cli

import (
	"context"
	"fmt"
	"time"

	"github.com/getnimbus/ultrago/u_logger"
	"github.com/getnimbus/ultrago/u_monitor"

	"feng-sui-core/internal/service"
)

func NewApp(
	syncTradeSvc service.SyncTradeService,
) App {
	return &app{
		syncTradeSvc: syncTradeSvc,
	}
}

type App interface {
	SyncTrades(ctx context.Context, rawParams ...string) error
}

type app struct {
	syncTradeSvc service.SyncTradeService
}

func (a *app) SyncTrades(ctx context.Context, rawParams ...string) error {
	ctx, logger := u_logger.GetLogger(ctx)

	defer u_monitor.TimeTrackWithCtx(ctx, time.Now())

	params, err := a.prepareParams(2, rawParams...)
	if err != nil {
		return err
	}

	switch params[0] {
	case "local":
		if err := a.syncTradeSvc.SyncTradesFromCsv(ctx, params[1]); err != nil {
			logger.Errorf("sync trades from csv failed: %v", err)
			return err
		}
		return nil
	case "s3":
		if err := a.syncTradeSvc.SyncTradesFromS3(ctx, params[1]); err != nil {
			logger.Errorf("sync trades from s3 failed: %v", err)
			return err
		}
		return nil
	default:
		logger.Infof("not supported flag")
		return nil
	}
}

func (a *app) prepareParams(requires int, params ...string) ([]string, error) {
	var results = make([]string, 0, len(params))
	for idx, param := range params {
		if idx < requires && param == "" {
			return nil, fmt.Errorf("param%d is missing", idx+1)
		}
		if param != "" {
			results = append(results, param)
		}
	}
	return results, nil
}
