DELETE FROM market_data;
DELETE FROM pools;
DELETE FROM analytics;
DELETE FROM verified_pools;
UPDATE cache_stats SET total_pools = 0, total_data_points = 0, last_global_update = datetime('now'), cache_size_bytes = 0 WHERE id = 1;
VACUUM;
