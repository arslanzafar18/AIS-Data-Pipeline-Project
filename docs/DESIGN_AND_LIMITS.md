# Main design decisions, trade-offs and limits
| Decision | Why | Trade-off / limit |
|---|---|---|
| Collector is a long-running service | The live API is an endless stream; Actions cannot run more often than every 5 min and may be delayed | Needs an always-on host; single collector = single point of failure |
| Bronze = raw text payload + lineage | Schema changes cannot break ingestion; full reproducibility | Parsing cost is paid in Silver |
| gzip JSONL, 120 s files | Simple, atomic, human-checkable | Not splittable; many small files (about 720/day) |
| Silver via Structured Streaming `availableNow` | Incremental, checkpointed, rerun-safe, no always-on Spark | Spark start-up (~10 s) per 2-min run; Parquet append has no ACID (Delta `MERGE` is the upgrade) |
| Reject vs correct split | Impossible positions are unusable; unreliable *attributes* are nulled but the position is kept | Thresholds are judgement calls (documented, counted) |
| Bounding-box prefilter before haversine | Measured 2.4x–6.4x faster join step, same output | Box assumes latitude <= 72 N |
| Gold gate + manifest | Consumers never see data that failed checks | Failure leaves Gold stale until fixed (alerted by monitor) |
| Fixed installations only | Sodir publishes fixed facilities; mobile rigs not available | Vessels near mobile rigs are not attributed |
## Known limits / assumptions
- AIS ship type does not reliably identify supply vessels; classification by name/MMSI list is future work.
- Live stream field names for `modelType=Full` are assumed; confirm with `scripts/infer_schema.py` on real data. Drift is counted, not fatal.
- The Sodir shapefile projection/column names are confirmed at first load (`reference/load_sodir.py` prints them).
- Benchmarks and the end-to-end run use SYNTHETIC data on a 1-core machine: they show relative effects and logic correctness, not real AIS volumes.
- Gold swap uses local-filesystem moves; on object storage use Delta or a manifest pointer.
- Monitoring runs on the self-hosted runner and reads the heartbeat and Gold manifest as local files. It cannot alert if the whole host is down (the runner goes offline and scheduled runs queue); an external check would need the JSON exposed over HTTPS.
