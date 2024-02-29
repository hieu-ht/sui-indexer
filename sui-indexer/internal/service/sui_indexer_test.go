package service

import (
	"context"
	"fmt"
	"testing"

	"github.com/smartystreets/goconvey/convey"
	"golang.org/x/exp/slices"

	"feng-sui-core/internal/conf"
)

func TestBlockSubscribeService(t *testing.T) {
	ctx := context.Background()

	_ = conf.LoadConfig(".")

	convey.FocusConvey("TestSuiIndexerService", t, func() {
		indexer, err := NewSuiIndexer()
		convey.So(err, convey.ShouldBeNil)

		convey.Convey("Fetch latest checkpoint", func() {
			res, err := indexer.FetchLatestCheckpoint(ctx)
			convey.So(err, convey.ShouldBeNil)

			fmt.Println(res)
		})

		convey.Convey("Fetch checkpoints", func() {
			res, err := indexer.FetchCheckpoints(ctx, "25081993", 100)
			convey.So(err, convey.ShouldBeNil)
			convey.So(len(res), convey.ShouldNotBeZeroValue)
			convey.So(true, convey.ShouldEqual, slices.Contains(res[0].Transactions, "9tqki1JHL2zCwVPSge5TCbqjquJpGzkQXXRX4qpeB9nL"))

			fmt.Println(res)
		})

		convey.FocusConvey("Fetch txs", func() {
			res, err := indexer.FetchTxs(ctx, []string{
				"9tqki1JHL2zCwVPSge5TCbqjquJpGzkQXXRX4qpeB9nL",
				"UrcvxPKvGS527FmdT8ir1hn6xA75Us1mVabS7zKiVYY",
				//"G39aMVvJ3nckxStXa6ox7xfvAd4uaUKCjSgfk3Bz4J8T",
				//"41hnYMezCvAsz1jjojC13TW9zz9A7TJE8e4zpc2u2Znh",
				//"4y7iJBnYtKUnpRCiAkKL2F2ffB4YnCZpWUY1K2FvRgy5",
				//"5gBCpwARZcqBn5ofp7gE2SPSLigxknLn4MHDCj5pGGB4",
				//"EP45NbKk1Yc7PoD21eaojmquw2ie7LedMyi63koCGjnU",
				//"6JLFWtM8FK1LCVSTH4n71nqZk4Jc5ydLgmK3Z9rvfUfk",
				//"AXhxPUbqipePRiv5i5phpmtyiXV9WCAA9CWWgrXSFDLr",
				//"ApQk7EmYqcqgckEhsikpgKGLq2tZapJsbHyCgYqxtQtr",
				//"Az8ityE1zLUtiPKrBGVbJ43h4B9iDQMu3VCkxyi9tyka",
				//"GA8KCy6fvw8Xm4xwtB5Jw9W2WgmANKiLgiWM5a5RQDTU",
			}...)
			convey.So(err, convey.ShouldBeNil)

			fmt.Println(res)
		})
	})
}
