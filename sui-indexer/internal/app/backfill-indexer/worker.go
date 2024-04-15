package backfill_indexer

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/alitto/pond"
	"github.com/avast/retry-go/v4"
	sui_client "github.com/coming-chat/go-sui/v2/client"
	"github.com/getnimbus/ultrago/u_logger"
	"github.com/samber/lo"
	"golang.org/x/sync/errgroup"
	"gorm.io/gorm"

	"feng-sui-core/internal/conf"
	"feng-sui-core/internal/entity"
	"feng-sui-core/internal/entity_dto/sui_model"
	"feng-sui-core/internal/repo"
	"feng-sui-core/internal/service"
)

func NewWorker(
	blockStatusRepo repo.BlockStatusRepo,
	baseSvc service.BaseService,
	s3Svc service.S3Service,
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
		blockStatusRepo:  blockStatusRepo,
		baseSvc:          baseSvc,
		s3Svc:            s3Svc,
		suiIndexer:       service.NewSuiIndexer(client, fallbackClient),
		limitCheckpoints: 20,
		numWorkers:       10,
		cooldown:         1 * time.Second,
		indexTopic:       conf.Config.SuiIndexTopic,
	}, nil
}

type worker struct {
	blockStatusRepo  repo.BlockStatusRepo
	baseSvc          service.BaseService
	s3Svc            service.S3Service
	suiIndexer       *service.SuiIndexer
	limitCheckpoints int
	numWorkers       int
	cooldown         time.Duration
	indexTopic       string
}

type Worker interface {
	FetchTxs(ctx context.Context) error
}

func (w *worker) FetchTxs(ctx context.Context) error {
	ctx, logger := u_logger.GetLogger(ctx)
	logger.Info("start fetching txs...")

	var (
		checkpointCh = make(chan *sui_model.Checkpoint, 300)
		txsCh        = make(chan []*sui_model.Transaction, 300)
	)
	defer func() {
		close(checkpointCh)
		close(txsCh)
	}()

	eg, childCtx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		if err := w.StoreS3(childCtx, checkpointCh, txsCh); err != nil {
			logger.Errorf("failed to store: %v", err)
		}
		return fmt.Errorf("stop stored goroutine") // force to stop goroutine
	})

	for i := 0; i < w.numWorkers; i++ {
		eg.Go(func() error {
			for {
				select {
				case <-childCtx.Done():
					logger.Infof("stop fetching txs!")
					return nil
				default:
					w.fetchTxs(childCtx, checkpointCh, txsCh)
					time.Sleep(w.cooldown) // cooldown api
				}
			}
		})
	}

	return eg.Wait()
}

func (w *worker) fetchTxs(ctx context.Context, checkpointCh chan<- *sui_model.Checkpoint, txsCh chan<- []*sui_model.Transaction) error {
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
			w.blockStatusRepo.S().ColumnEqual("chain", "SUI"),
			w.blockStatusRepo.S().FilterType(entity.BlockStatusType_BACKFILL),
			w.blockStatusRepo.S().Limit(w.limitCheckpoints),
			w.blockStatusRepo.S().SortBy("block_number", "ASC"),
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
			return w.suiIndexer.FetchCheckpoints(ctx, strconv.FormatInt(checkpointIds[0].BlockNumber, 10), strconv.FormatInt(checkpointIds[len(checkpointIds)-1].BlockNumber, 10))
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

				var parsedTxs = make([]*sui_model.Transaction, 0)
				for _, tx := range txs {
					if err := tx.Validate(); err != nil {
						logger.Warnf("invalid tx: %v", err)
						continue
					}
					parsedTxs = append(parsedTxs, tx.WithDateKey())
				}

				// send txs to kafka
				if len(parsedTxs) > 0 {
					txsCh <- parsedTxs
				}

				// send checkpoints to kafka
				if checkpoint != nil {
					checkpointCh <- checkpoint
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

func (w *worker) StoreS3(ctx context.Context, checkpointCh <-chan *sui_model.Checkpoint, txsCh <-chan []*sui_model.Transaction) error {
	ctx, logger := u_logger.GetLogger(ctx)
	logger.Info("start goroutine store s3...")

	pool := pond.New(100, 0, pond.MinWorkers(10))
	defer pool.StopAndWait()

	for {
		select {
		case <-ctx.Done():
			return nil
		case c, ok := <-checkpointCh:
			if !ok {
				logger.Info("channel is closed")
				return nil
			}
			checkpoint := c
			if checkpoint == nil {
				continue
			}

			pool.Submit(func() {
				errCh := make(chan error, 1)
				defer close(errCh)

				pw := w.s3Svc.FileStreamWriter(ctx, conf.Config.AwsBucket, fmt.Sprintf("checkpoints/sui-checkpoints/datekey=%v/%v.json.gz", checkpoint.DateKey, checkpoint.SequenceNumber), errCh)
				zw, err := gzip.NewWriterLevel(pw, gzip.BestSpeed)
				if err != nil {
					logger.Errorf("failed to create gzip writer: %v", err)
					return
				}

				// add data to gzip
				data, err := json.Marshal(checkpoint)
				if err != nil {
					logger.Errorf("failed to marshal checkpoint: %v", err)
					return
				}
				_, err = zw.Write(data)
				if err != nil {
					logger.Errorf("failed to write checkpoint to S3: %v", err)
					return
				}

				// flush data to S3
				zw.Close()
				pw.Close()

				returnErr := <-errCh
				if returnErr != nil {
					logger.Errorf("failed to store checkpoint to S3: %v", err)
					return
				}
				logger.Infof("[%v] submit checkpoint %v to S3 success", checkpoint.DateKey, checkpoint.SequenceNumber)
			})
		case t, ok := <-txsCh:
			if !ok {
				logger.Info("channel is closed")
				return nil
			}
			txs := t
			if len(txs) == 0 {
				continue
			}

			pool.Submit(func() {
				errCh := make(chan error, 1)
				defer close(errCh)

				pw := w.s3Svc.FileStreamWriter(ctx, conf.Config.AwsBucket, fmt.Sprintf("txs/sui-txs/datekey=%v/%v.json.gz", txs[0].DateKey, txs[0].Checkpoint), errCh)
				zw, err := gzip.NewWriterLevel(pw, gzip.BestSpeed)
				if err != nil {
					logger.Errorf("failed to create gzip writer: %v", err)
					return
				}

				// add data to gzip
				for _, tx := range txs {
					data, err := json.Marshal(tx)
					if err != nil {
						logger.Errorf("failed to marshal tx: %v", err)
						return
					}
					_, err = zw.Write(data)
					if err != nil {
						logger.Errorf("failed to write tx to S3: %v", err)
						return
					}
					zw.Write([]byte("\n"))
				}

				// flush data to S3
				zw.Close()
				pw.Close()

				returnErr := <-errCh
				if returnErr != nil {
					logger.Errorf("failed to store tx to S3: %v", err)
					return
				}
				logger.Infof("[%v] submit txs in checkpoint %v to S3 success", txs[0].DateKey, txs[0].Checkpoint)
			})
		}
	}
}

// TODO: not used kafka for backfill for fast growth kafka cluster (disk performance issues)
//func (w *worker) StoreKafka(ctx context.Context, checkpointCh <-chan *sui_model.Checkpoint, txsCh <-chan []*sui_model.Transaction, eventsCh <-chan []*sui_model.Event, indicesCh <-chan []*entity.SuiIndex) error {
//	ctx, logger := u_logger.GetLogger(ctx)
//	logger.Info("start goroutine store kafka...")
//
//	pool := pond.New(100, 0, pond.MinWorkers(10))
//	defer pool.StopAndWait()
//
//	for {
//		select {
//		case <-ctx.Done():
//			return nil
//		case c, ok := <-checkpointCh:
//			if !ok {
//				logger.Info("channel is closed")
//				return nil
//			}
//
//			checkpoint := c
//			if checkpoint == nil {
//				continue
//			}
//			pool.Submit(func() {
//				if err := w.kafkaProducer.SendJson(ctx, conf.Config.SuiCheckpointsTopic, checkpoint); err != nil {
//					logger.Errorf("failed to send payload to kafka sui checkpoints topic: %v", err)
//				}
//			})
//		case t, ok := <-txsCh:
//			if !ok {
//				logger.Info("channel is closed")
//				return nil
//			}
//
//			txs := t
//			if len(txs) == 0 {
//				continue
//			}
//			for _, t := range txs {
//				tx := t
//				pool.Submit(func() {
//					if err := w.kafkaProducer.SendJson(ctx, conf.Config.SuiTxsTopic, tx.WithDateKey()); err != nil {
//						logger.Errorf("failed to send payload to kafka sui txs topic: %v", err)
//					}
//				})
//			}
//		case e, ok := <-eventsCh:
//			if !ok {
//				logger.Info("channel is closed")
//				return nil
//			}
//
//			events := e
//			if len(events) == 0 {
//				continue
//			}
//			for _, e := range events {
//				event := e
//				pool.Submit(func() {
//					if err := w.kafkaProducer.SendJson(ctx, conf.Config.SuiEventsTopic, event); err != nil {
//						logger.Errorf("failed to send payload to kafka sui events topic: %v", err)
//					}
//				})
//			}
//		case i, ok := <-indicesCh:
//			if !ok {
//				logger.Info("channel is closed")
//				return nil
//			}
//
//			indices := i
//			if len(indices) == 0 {
//				continue
//			}
//			pool.Submit(func() {
//				for _, index := range indices {
//					if err := w.kafkaProducer.SendJson(
//						ctx,
//						conf.Config.SuiIndexTopic,
//						index.ToKafka(),
//					); err != nil {
//						logger.Errorf("failed to send payload to kafka sui index topic: %v", err)
//					}
//				}
//			})
//		}
//	}
//}
