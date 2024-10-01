package queries

func GetLongRunningQueries() string {
	return `
	SELECT
	    pid,
	    user,
	    pg_stat_activity.query_start,
	    now() - pg_stat_activity.query_start AS query_time,
	    query,
	    state,
	    wait_event_type,
	    wait_event
	FROM pg_stat_activity
	WHERE (now() - pg_stat_activity.query_start) > interval '5 minutes' AND state != 'idle';
`
}

func GetQueriesWithLockStatus() string {
	return `
SELECT count(pg_stat_activity.pid) AS number_of_queries,
       substring(trim(LEADING
                      FROM regexp_replace(pg_stat_activity.query, '[\n\r]+'::text,
                                          ' '::text, 'g'::text))
                 FROM 0
                 FOR 200) AS query_name,
       max(age(CURRENT_TIMESTAMP, query_start)) AS max_wait_time,
       wait_event,
       usename,
       locktype,
       mode,
       granted
FROM pg_stat_activity
         LEFT JOIN pg_locks ON pg_stat_activity.pid = pg_locks.pid
WHERE query != '<IDLE>'
  AND query NOT ILIKE '%pg_%' AND query NOT ILIKE '%application_name%' AND query NOT ILIKE '%inet%'
    AND age(CURRENT_TIMESTAMP, query_start) > '5 milliseconds'::interval
GROUP BY query_name,
    wait_event,
    usename,
    locktype,
    mode,
    granted
ORDER BY max_wait_time DESC;
`
}

func GetTupleInfo() string {
	return `
SELECT relname as "relation", EXTRACT (EPOCH FROM current_timestamp-last_autovacuum) as since_last_av,
       autovacuum_count as av_count, n_tup_ins, n_tup_upd, n_tup_del, n_live_tup, n_dead_tup
FROM pg_stat_all_tables
WHERE schemaname = 'public'
ORDER BY relname;
`
}

func GetIndexUsage() string {
	return `
SELECT
    t.tablename AS "relation",
    indexname,
    c.reltuples AS num_rows,
    pg_size_pretty(pg_relation_size(quote_ident(t.tablename)::text)) AS table_size,
    pg_size_pretty(pg_relation_size(quote_ident(indexrelname)::text)) AS index_size,
    idx_scan AS number_of_scans,
    idx_tup_read AS tuples_read,
    idx_tup_fetch AS tuples_fetched
FROM pg_tables t
         LEFT OUTER JOIN pg_class c ON t.tablename=c.relname
         LEFT OUTER JOIN
     ( SELECT c.relname AS ctablename, ipg.relname AS indexname, x.indnatts AS number_of_columns, idx_scan, idx_tup_read, idx_tup_fetch, indexrelname, indisunique FROM pg_index x
                                                                                                                                                                            JOIN pg_class c ON c.oid = x.indrelid
                                                                                                                                                                            JOIN pg_class ipg ON ipg.oid = x.indexrelid
                                                                                                                                                                            JOIN pg_stat_all_indexes psai ON x.indexrelid = psai.indexrelid )
         AS foo
     ON t.tablename = foo.ctablename
WHERE t.schemaname='public'
ORDER BY 1,2;
`
}

func GetCachedTables() string {
	return `
SELECT relname AS "relation",
       heap_blks_read AS heap_read,
       heap_blks_hit AS heap_hit,
       ( (heap_blks_hit*100) / NULLIF((heap_blks_hit + heap_blks_read), 0)) AS ratio
FROM pg_statio_user_tables;
`
}

func GetCachedTotal() string {
	return `
SELECT sum(heap_blks_read) AS heap_read,
       sum(heap_blks_hit)  AS heap_hit,
       (sum(heap_blks_hit)*100 / NULLIF((sum(heap_blks_hit) + sum(heap_blks_read)),0)) AS ratio
FROM pg_statio_user_tables;
`
}

func GetDiskUsages() string {
	return `
SELECT relname AS "relation",
       pg_size_pretty(pg_total_relation_size(C.oid)) AS "total_size"
FROM pg_class C
         LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
WHERE nspname NOT IN ('pg_catalog', 'information_schema')
  AND C.relkind <> 'i'
  AND nspname ='public'
ORDER BY pg_total_relation_size(C.oid) DESC;
`
}

func GetRelationSizes() string {
	return `
SELECT relname AS "relation",
       pg_size_pretty(pg_relation_size(C.oid)) AS "size"
FROM pg_class C
         LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
WHERE nspname = 'public'
ORDER BY pg_relation_size(C.oid) DESC;
`
}

func GetDBSize() string {
	return `SELECT pg_size_pretty(pg_database_size(current_database()));`
}

func GetTableAndIndexBloat() string {
	return `
SELECT
    tablename AS "relation", reltuples::bigint AS tups, relpages::bigint AS pages, otta,
    ROUND(CASE WHEN otta=0 OR sml.relpages=0 OR sml.relpages=otta THEN 0.0 ELSE sml.relpages/otta::numeric END,1) AS tbloat,
    CASE WHEN relpages < otta THEN 0 ELSE relpages::bigint - otta END AS wastedpages,
  CASE WHEN relpages < otta THEN 0 ELSE bs*(sml.relpages-otta)::bigint END AS wastedbytes,
  CASE WHEN relpages < otta THEN 0 ELSE (bs*(relpages-otta))::bigint END AS wastedsize,
  iname, ituples::bigint AS itups, ipages::bigint AS ipages, iotta,
  ROUND(CASE WHEN iotta=0 OR ipages=0 OR ipages=iotta THEN 0.0 ELSE ipages/iotta::numeric END,1) AS ibloat,
  CASE WHEN ipages < iotta THEN 0 ELSE ipages::bigint - iotta END AS wastedipages,
  CASE WHEN ipages < iotta THEN 0 ELSE bs*(ipages-iotta) END AS wastedibytes,
  CASE WHEN ipages < iotta THEN 0 ELSE (bs*(ipages-iotta))::bigint END AS wastedisize,
  CASE WHEN relpages < otta THEN
    CASE WHEN ipages < iotta THEN 0 ELSE bs*(ipages-iotta::bigint) END
    ELSE CASE WHEN ipages < iotta THEN bs*(relpages-otta::bigint)
      ELSE bs*(relpages-otta::bigint + ipages-iotta::bigint) END
  END AS totalwastedbytes
FROM (
    SELECT
    nn.nspname AS schemaname,
    cc.relname AS tablename,
    COALESCE(cc.reltuples,0) AS reltuples,
    COALESCE(cc.relpages,0) AS relpages,
    COALESCE(bs,0) AS bs,
    COALESCE(CEIL((cc.reltuples*((datahdr+ma-
    (CASE WHEN datahdr%ma=0 THEN ma ELSE datahdr%ma END))+nullhdr2+4))/(bs-20::float)),0) AS otta,
    COALESCE(c2.relname,'?') AS iname, COALESCE(c2.reltuples,0) AS ituples, COALESCE(c2.relpages,0) AS ipages,
    COALESCE(CEIL((c2.reltuples*(datahdr-12))/(bs-20::float)),0) AS iotta -- very rough approximation, assumes all cols
    FROM
    pg_class cc
    JOIN pg_namespace nn ON cc.relnamespace = nn.oid AND nn.nspname = 'public'
    LEFT JOIN
    (
    SELECT
    ma,bs,foo.nspname,foo.relname,
    (datawidth+(hdr+ma-(case when hdr%ma=0 THEN ma ELSE hdr%ma END)))::numeric AS datahdr,
    (maxfracsum*(nullhdr+ma-(case when nullhdr%ma=0 THEN ma ELSE nullhdr%ma END))) AS nullhdr2
    FROM (
    SELECT
    ns.nspname, tbl.relname, hdr, ma, bs,
    SUM((1-coalesce(null_frac,0))*coalesce(avg_width, 2048)) AS datawidth,
    MAX(coalesce(null_frac,0)) AS maxfracsum,
    hdr+(
    SELECT 1+count(*)/8
    FROM pg_stats s2
    WHERE null_frac<>0 AND s2.schemaname = ns.nspname AND s2.tablename = tbl.relname
    ) AS nullhdr
    FROM pg_attribute att
    JOIN pg_class tbl ON att.attrelid = tbl.oid
    JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
    LEFT JOIN pg_stats s ON s.schemaname=ns.nspname
    AND s.tablename = tbl.relname
    AND s.inherited=false
    AND s.attname=att.attname,
    (
    SELECT
    (SELECT current_setting('block_size')::numeric) AS bs,
    CASE WHEN SUBSTRING(SPLIT_PART(v, ' ', 2) FROM '#"[0-9]+.[0-9]+#"%' for '#')
    IN ('8.0','8.1','8.2') THEN 27 ELSE 23 END AS hdr,
    CASE WHEN v ~ 'mingw32' OR v ~ '64-bit' THEN 8 ELSE 4 END AS ma
    FROM (SELECT version() AS v) AS foo
    ) AS constants
    WHERE att.attnum > 0 AND tbl.relkind='r'
    GROUP BY 1,2,3,4,5
    ) AS foo
    ) AS rs
    ON cc.relname = rs.relname AND nn.nspname = rs.nspname AND nn.nspname = 'public'
    LEFT JOIN pg_index i ON indrelid = cc.oid
    LEFT JOIN pg_class c2 ON c2.oid = i.indexrelid
    ) AS sml;
`
}
