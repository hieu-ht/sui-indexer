## SUI Ingest

SUI Ingest infrastructure

![CleanShot 2024-02-23 at 17 32 31@2x](https://github.com/getnimbus/sui-indexer/assets/9281080/77f576ba-55a5-4bf0-8bee-56457bfbf625)

To write new SUI data ingest, you need to add a config following TS `IndexerConfig` on https://github.com/getnimbus/sui-indexer/blob/main/sui-ingest/swap/src/type.ts


### Realtime ingest
We keep the real-time data for 7 days so you can ingest the data in real time.

Example ingestion swap from multiple platforms: https://github.com/getnimbus/sui-indexer/blob/main/sui-ingest/swap/src/main.ts

### Backfill ingest
Please contact us at thanhle@getnimbus.io or @thanhle27 on TG
