package sui_worker

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/avast/retry-go/v4"
	sui_client "github.com/coming-chat/go-sui/v2/client"
	"github.com/coming-chat/go-sui/v2/types"
	"github.com/getnimbus/ultrago/u_logger"
	"github.com/samber/lo"
	"golang.org/x/sync/errgroup"
	"gorm.io/gorm"

	"feng-sui-core/internal/conf"
	"feng-sui-core/internal/entity"
	"feng-sui-core/internal/entity_dto/sui_model"
	"feng-sui-core/internal/infra"
	"feng-sui-core/internal/repo"
	"feng-sui-core/internal/service"
)

func NewWorker(
	kafkaProducer infra.KafkaSyncProducer,
	blockStatusRepo repo.BlockStatusRepo,
	baseSvc service.BaseService,
) (Worker, error) {
	var transport *http.Transport
	if conf.Config.IsUseProxy() {
		proxyUrl, err := url.Parse(conf.Config.HttpProxy)
		if err != nil {
			return nil, err
		}
		transport = &http.Transport{Proxy: http.ProxyURL(proxyUrl)}
	} else {
		transport = http.DefaultTransport.(*http.Transport)
	}

	client, err := sui_client.DialWithClient(conf.Config.SuiRpc, &http.Client{
		Transport: transport.Clone(),
		Timeout:   2 * 60 * time.Second, // 2 mins
	})
	if err != nil {
		return nil, err
	}
	fallbackClient, _ := sui_client.DialWithClient(conf.Config.FallbackSuiRpc, &http.Client{
		Transport: transport.Clone(),
		Timeout:   2 * 60 * time.Second, // 2 mins
	})

	return &worker{
		kafkaProducer:    kafkaProducer,
		blockStatusRepo:  blockStatusRepo,
		baseSvc:          baseSvc,
		suiIndexer:       service.NewSuiIndexer(client, fallbackClient),
		limitCheckpoints: 10,
		numWorkers:       10, // 1 checkpoint/s
		cooldown:         5 * time.Second,
		checkpointsTopic: conf.Config.SuiCheckpointsTopic,
		txsTopic:         conf.Config.SuiTxsTopic,
		eventsTopic:      conf.Config.SuiEventsTopic,
		indexTopic:       conf.Config.SuiIndexTopic,
	}, nil
}

type worker struct {
	kafkaProducer    infra.KafkaSyncProducer
	blockStatusRepo  repo.BlockStatusRepo
	baseSvc          service.BaseService
	suiIndexer       *service.SuiIndexer
	limitCheckpoints int
	numWorkers       int
	cooldown         time.Duration
	checkpointsTopic string
	txsTopic         string
	eventsTopic      string
	indexTopic       string
}

type Worker interface {
	FetchTxs(ctx context.Context) error
}

func (w *worker) FetchTxs(ctx context.Context) error {
	ctx, logger := u_logger.GetLogger(ctx)
	logger.Info("start fetching txs...")

	eg, childCtx := errgroup.WithContext(ctx)
	for i := 0; i < w.numWorkers; i++ {
		eg.Go(func() error {
			for {
				select {
				case <-childCtx.Done():
					logger.Infof("stop fetching txs!")
					return nil
				default:
					w.fetchTxs(childCtx)
					time.Sleep(w.cooldown) // cooldown api
				}
			}
		})
	}

	return eg.Wait()
}

func (w *worker) fetchTxs(ctx context.Context) error {
	ctx, logger := u_logger.GetLogger(ctx)
	logger.Info("waiting query from db...")

	var (
		checkpointIds []*entity.BlockStatus
		err           error
	)

	// recover panic
	defer func() {
		if re := recover(); re != nil {
			logger.Infof("panic: %v", re)
			// update status FAIL
			w.updateCheckpointStatus(ctx, entity.BlockStatus_FAIL, checkpointIds...)
			return
		}
	}()

	defer func() {
		if err != nil {
			// update status FAIL
			w.updateCheckpointStatus(ctx, entity.BlockStatus_FAIL, checkpointIds...)
			return
		}
		// update status DONE
		w.updateCheckpointStatus(ctx, entity.BlockStatus_DONE, checkpointIds...)
	}()

	err = w.baseSvc.ExecTx(ctx, func(txCtx context.Context) error {
		scopes := []func(db *gorm.DB) *gorm.DB{
			w.blockStatusRepo.S().Locking(),
			w.blockStatusRepo.S().FilterStatuses(
				entity.BlockStatus_NOT_READY,
				entity.BlockStatus_FAIL,
			),
			w.blockStatusRepo.S().FilterType(entity.BlockStatusType_REALTIME),
			w.blockStatusRepo.S().ColumnEqual("chain", "SUI"),
			w.blockStatusRepo.S().Limit(w.limitCheckpoints),
			//w.blockStatusRepo.S().SortBy("block_number", "DESC"), // comment out for performance problem
		}

		checkpointIds, err = w.blockStatusRepo.GetList(txCtx, scopes...)
		if err != nil {
			return err
		}

		// update block status PROCESSING
		return w.updateCheckpointStatus(txCtx, entity.BlockStatus_PROCESSING, checkpointIds...)
	})
	if err != nil {
		logger.Errorf("failed to fetch checkpoint ids: %v", err)
		return fmt.Errorf("failed to fetch checkpoint ids: %v", err)
	}

	if len(checkpointIds) == 0 {
		return nil
	}

	var checkpoints []*sui_model.Checkpoint
	checkpoints, err = retry.DoWithData(
		func() ([]*sui_model.Checkpoint, error) {
			return w.suiIndexer.FetchCheckpoints(ctx, strconv.FormatInt(checkpointIds[0].BlockNumber, 10), w.limitCheckpoints)
		},
		// retry configs
		[]retry.Option{
			retry.Attempts(uint(5)),
			retry.OnRetry(func(n uint, err error) {
				logger.Errorf("Retry invoke function %d to and get error: %v", n+1, err)
			}),
			retry.Delay(3 * time.Second),
			retry.Context(ctx),
		}...,
	)
	if err != nil {
		logger.Errorf("failed to fetch checkpoint from %v: %v", checkpointIds[0].BlockNumber, err)
		return fmt.Errorf("failed to fetch checkpoint from %v: %v", checkpointIds[0].BlockNumber, err)
	}

	eg, childCtx := errgroup.WithContext(ctx)
	for _, c := range checkpoints {
		checkpoint := c.WithDateKey()
		if err := checkpoint.Validate(); err != nil {
			logger.Warnf("invalid checkpoint: %v", err)
			continue
		}

		eg.Go(func() error {
			chunkTxDigests := lo.Chunk(checkpoint.Transactions, 30)
			for _, txDigests := range chunkTxDigests {
				txs, err := retry.DoWithData(
					func() ([]*sui_model.Transaction, error) {
						return w.suiIndexer.FetchTxs(childCtx, txDigests...)
					},
					// retry configs
					[]retry.Option{
						retry.Attempts(uint(5)),
						retry.OnRetry(func(n uint, err error) {
							logger.Errorf("Retry invoke function %d to and get error: %v", n+1, err)
						}),
						retry.Delay(3 * time.Second),
						retry.Context(childCtx),
					}...,
				)
				if err != nil {
					return err
				}
				if err := checkpoint.SetBloomFilter(txs); err != nil {
					return err
				}

				var (
					parsedTxs    = make([]*sui_model.Transaction, 0)
					parsedEvents = make([]*sui_model.Event, 0)
					indices      = make([]*entity.SuiIndex, 0)
				)
				for _, tx := range txs {
					if err := tx.Validate(); err != nil {
						logger.Warnf("invalid tx: %v", err)
						continue
					}
					parsedTxs = append(parsedTxs, tx.WithDateKey())

					// extract tx gasUsed for events
					gasUsed := tx.Effects["gasUsed"]
					for _, event := range tx.Events {
						if event.TimestampMs == nil || event.TimestampMs.Int64() == 0 {
							parsedTs, err := strconv.ParseUint(tx.TimestampMs, 10, 64)
							if err != nil {
								logger.Errorf("failed to parse timestamp: %v", err)
								return err
							}
							ts := types.NewSafeSuiBigInt[uint64](parsedTs)
							event.TimestampMs = &ts
						}
						parsedEvent := &sui_model.Event{
							SuiEvent: event,
						}
						parsedEvent = parsedEvent.
							WithDateKey().
							WithCheckpoint(checkpoint.SequenceNumber).
							WithGasUsed(gasUsed)

						parsedEvents = append(parsedEvents, parsedEvent)

						checkpointSeq, _ := strconv.ParseInt(checkpoint.SequenceNumber, 10, 64)
						suiIndex := &entity.SuiIndex{
							DateKey:          checkpoint.DateKey,
							CheckpointDigest: checkpoint.Digest,
							CheckpointSeq:    checkpointSeq,
							TxDigest:         tx.Digest,
							EventSeq:         event.Id.EventSeq.Int64(),
							PackageId:        event.PackageId.String(),
							EventType:        event.Type,
						}

						indices = append(indices, suiIndex)
					}
				}

				eg2, childCtx2 := errgroup.WithContext(childCtx)
				// send txs to kafka
				eg2.Go(func() error {
					for _, tx := range parsedTxs {
						if err := w.kafkaProducer.SendJson(childCtx2, w.txsTopic, tx); err != nil {
							logger.Errorf("failed to send payload to kafka sui txs topic: %v", err)
							return err
						}
					}
					return nil
				})

				// send events to kafka
				eg2.Go(func() error {
					for _, event := range parsedEvents {
						if err := w.kafkaProducer.SendJson(childCtx2, w.eventsTopic, event); err != nil {
							logger.Errorf("failed to send payload to kafka sui events topic: %v", err)
							return err
						}
					}
					return nil
				})

				// send sui index to kafka
				eg2.Go(func() error {
					for _, index := range indices {
						if err := w.kafkaProducer.SendJson(
							childCtx2,
							w.indexTopic,
							index.ToKafka(),
						); err != nil {
							logger.Errorf("failed to send payload to kafka sui index topic: %v", err)
							return err
						}
					}
					return nil
				})

				// send checkpoints to kafka
				eg2.Go(func() error {
					if checkpoint != nil {
						if err := w.kafkaProducer.SendJson(childCtx2, w.checkpointsTopic, checkpoint); err != nil {
							logger.Errorf("failed to send payload to kafka sui checkpoints topic: %v", err)
							return err
						}
					}
					return nil
				})

				if err := eg2.Wait(); err != nil {
					return err
				}
			}
			return nil
		})
	}

	err = eg.Wait()
	if err != nil {
		logger.Errorf("failed to fetch txs from checkpoint %v: %v", checkpointIds[0].BlockNumber, err)
		return fmt.Errorf("failed to fetch txs from checkpoint %v: %v", checkpointIds[0].BlockNumber, err)
	}
	return nil
}

func (w *worker) updateCheckpointStatus(ctx context.Context, status int, checkpointIds ...*entity.BlockStatus) error {
	if len(checkpointIds) == 0 {
		return nil
	}
	if err := w.blockStatusRepo.UpdateStatus(ctx, status, lo.Map(checkpointIds, func(item *entity.BlockStatus, _ int) string {
		return item.ID
	})...); err != nil {
		return err
	}
	return nil
}
