#Investigation: Sortkey skew leading to excessive block I/O with early materialization 

*Authored by chriz-bigdata on 2016-11-08*

Redshift leverages an [early materialization strategy](http://db.lcs.mit.edu/projects/cstore/abadiicde2007.pdf), as our testing has found it to be a performant option for the vast
majority of scans for Redshift workloads. The details of this investigation will show how certain edge cases where excessive compression is involved can lead to a performance degradation, relative to 
less compression, or no compression at all.

##Queries
```sql
SELECT * FROM exercise_10.query_1; -- Delta encoding, query ID = 320310, runtime: 280.088 ms
SELECT * FROM exercise_10.query_2; -- Runlength encoding, query ID = 320312, runtime: 2706.043 ms
```

##Relations
Two views with identical definitions, deviating only by the table which they scan.
```sql
dev=# \d+ exercise_10.query_1
                                           View "exercise_10.query_1"
     Column     |     Type      | Encoding | DistKey | SortKey | Preload | Encryption | Modifiers | Description
----------------+---------------+----------+---------+---------+---------+------------+-----------+-------------
 l_returnflag   | character(1)  | none     | f       | 0       | f       | none       |           | f
 l_linestatus   | character(1)  | none     | f       | 0       | f       | none       |           | f
 sum_qty        | numeric(38,2) | none     | f       | 0       | f       | none       |           | f
 sum_base_price | numeric(38,2) | none     | f       | 0       | f       | none       |           | f
 sum_disc_price | numeric(38,2) | none     | f       | 0       | f       | none       |           | f
 sum_charge     | numeric(38,2) | none     | f       | 0       | f       | none       |           | f
 avg_qty        | numeric(38,2) | none     | f       | 0       | f       | none       |           | f
 avg_price      | numeric(38,2) | none     | f       | 0       | f       | none       |           | f
 avg_disc       | numeric(38,2) | none     | f       | 0       | f       | none       |           | f
 count_order    | bigint        | none     | f       | 0       | f       | none       |           | f
View definition:
 SELECT lineitem.l_returnflag, lineitem.l_linestatus, sum(lineitem.l_quantity) AS sum_qty, sum(lineitem.l_extendedprice) AS sum_base_price, sum(lineitem.l_extendedprice * (1::numeric::numeric(18,0) - lineitem.l_discount))::numeric(38,2) AS sum_disc_price, sum(lineitem.l_extendedprice::numeric(38,2) * (1::numeric::numeric(18,0) - lineitem.l_discount) * (1::numeric::numeric(18,0) + lineitem.l_tax))::numeric(38,2) AS sum_charge, avg(lineitem.l_quantity) AS avg_qty, avg(lineitem.l_extendedprice) AS avg_price, avg(lineitem.l_discount) AS avg_disc, count(*) AS count_order
   FROM lineitem
  WHERE lineitem.l_shipdate <= ('1998-12-01'::date - '66 days'::interval)::date AND lineitem.l_shipdate >= ('1998-12-01'::date - '68 days'::interval)::date
  GROUP BY lineitem.l_returnflag, lineitem.l_linestatus
  ORDER BY lineitem.l_returnflag, lineitem.l_linestatus;

dev=# \d+ exercise_10.query_2
                                           View "exercise_10.query_2"
     Column     |     Type      | Encoding | DistKey | SortKey | Preload | Encryption | Modifiers | Description
----------------+---------------+----------+---------+---------+---------+------------+-----------+-------------
 l_returnflag   | character(1)  | none     | f       | 0       | f       | none       |           | f
 l_linestatus   | character(1)  | none     | f       | 0       | f       | none       |           | f
 sum_qty        | numeric(38,2) | none     | f       | 0       | f       | none       |           | f
 sum_base_price | numeric(38,2) | none     | f       | 0       | f       | none       |           | f
 sum_disc_price | numeric(38,2) | none     | f       | 0       | f       | none       |           | f
 sum_charge     | numeric(38,2) | none     | f       | 0       | f       | none       |           | f
 avg_qty        | numeric(38,2) | none     | f       | 0       | f       | none       |           | f
 avg_price      | numeric(38,2) | none     | f       | 0       | f       | none       |           | f
 avg_disc       | numeric(38,2) | none     | f       | 0       | f       | none       |           | f
 count_order    | bigint        | none     | f       | 0       | f       | none       |           | f
View definition:
 SELECT lineitem.l_returnflag, lineitem.l_linestatus, sum(lineitem.l_quantity) AS sum_qty, sum(lineitem.l_extendedprice) AS sum_base_price, sum(lineitem.l_extendedprice * (1::numeric::numeric(18,0) - lineitem.l_discount))::numeric(38,2) AS sum_disc_price, sum(lineitem.l_extendedprice::numeric(38,2) * (1::numeric::numeric(18,0) - lineitem.l_discount) * (1::numeric::numeric(18,0) + lineitem.l_tax))::numeric(38,2) AS sum_charge, avg(lineitem.l_quantity) AS avg_qty, avg(lineitem.l_extendedprice) AS avg_price, avg(lineitem.l_discount) AS avg_disc, count(*) AS count_order
   FROM exercise_10.lineitem
  WHERE lineitem.l_shipdate <= ('1998-12-01'::date - '66 days'::interval)::date AND lineitem.l_shipdate >= ('1998-12-01'::date - '68 days'::interval)::date
  GROUP BY lineitem.l_returnflag, lineitem.l_linestatus
  ORDER BY lineitem.l_returnflag, lineitem.l_linestatus;
  
dev=# select * from svv_table_info where table_id IN (select distinct tbl from stl_scan where query IN (320310,320312));
 database |   schema    | table_id |  table   | encoded |    diststyle    |  sortkey1  | max_varchar | sortkey1_enc | sortkey_num | size  | pct_used | empty | unsorted | stats_off | tbl_rows  | skew_sortkey1 | skew_rows
----------+-------------+----------+----------+---------+-----------------+------------+-------------+--------------+-------------+-------+----------+-------+----------+-----------+-----------+---------------+-----------
 dev      | exercise_10 |   108338 | lineitem | Y       | KEY(l_orderkey) | l_shipdate |          44 | runlength    |           1 | 26133 |   4.5695 |     0 |     0.00 |      0.00 | 600037902 |        393.83 |      1.00
 dev      | public      |   108315 | lineitem | Y       | KEY(l_orderkey) | l_shipdate |          44 | delta        |           1 | 26697 |   4.6681 |     0 |     0.00 |      0.00 | 600037902 |         12.18 |      1.00
(2 rows)
```

The underlying tables `exercise_10.lineitem` and `public.lineitem` contain identical data and schema, the only difference being that the sortkey column `l_shipdate` is encoded with delta encoding in `public.lineitem` and runlength encoding in `exercise_10.lineitem`.

```sql
dev=# \d+ exercise_10.lineitem
                                               Table "exercise_10.lineitem"
     Column      |         Type          | Encoding  | DistKey | SortKey | Preload | Encryption | Modifiers | Description
-----------------+-----------------------+-----------+---------+---------+---------+------------+-----------+-------------
 l_orderkey      | bigint                | mostly32  | t       | 0       | f       | none       | not null  | t
 l_partkey       | bigint                | mostly32  | f       | 0       | f       | none       | not null  | t
 l_suppkey       | integer               | none      | f       | 0       | f       | none       | not null  | t
 l_linenumber    | integer               | delta     | f       | 0       | f       | none       | not null  | t
 l_quantity      | numeric(12,2)         | bytedict  | f       | 0       | f       | none       | not null  | t
 l_extendedprice | numeric(12,2)         | mostly32  | f       | 0       | f       | none       | not null  | t
 l_discount      | numeric(12,2)         | delta     | f       | 0       | f       | none       | not null  | t
 l_tax           | numeric(12,2)         | delta     | f       | 0       | f       | none       | not null  | t
 l_returnflag    | character(1)          | lzo       | f       | 0       | f       | none       | not null  | t
 l_linestatus    | character(1)          | lzo       | f       | 0       | f       | none       | not null  | t
 l_shipdate      | date                  | runlength | f       | 1       | f       | none       | not null  | t
 l_commitdate    | date                  | delta     | f       | 0       | f       | none       | not null  | t
 l_receiptdate   | date                  | delta     | f       | 0       | f       | none       | not null  | t
 l_shipinstruct  | character(25)         | bytedict  | f       | 0       | f       | none       | not null  | t
 l_shipmode      | character(10)         | bytedict  | f       | 0       | f       | none       | not null  | t
 l_comment       | character varying(44) | text255   | f       | 0       | f       | none       | not null  | t
Has OIDs: yes

dev=# \d+ lineitem
                                                 Table "public.lineitem"
     Column      |         Type          | Encoding | DistKey | SortKey | Preload | Encryption | Modifiers | Description
-----------------+-----------------------+----------+---------+---------+---------+------------+-----------+-------------
 l_orderkey      | bigint                | mostly32 | t       | 0       | f       | none       | not null  | t
 l_partkey       | bigint                | mostly32 | f       | 0       | f       | none       | not null  | t
 l_suppkey       | integer               | none     | f       | 0       | f       | none       | not null  | t
 l_linenumber    | integer               | delta    | f       | 0       | f       | none       | not null  | t
 l_quantity      | numeric(12,2)         | bytedict | f       | 0       | f       | none       | not null  | t
 l_extendedprice | numeric(12,2)         | mostly32 | f       | 0       | f       | none       | not null  | t
 l_discount      | numeric(12,2)         | delta    | f       | 0       | f       | none       | not null  | t
 l_tax           | numeric(12,2)         | delta    | f       | 0       | f       | none       | not null  | t
 l_returnflag    | character(1)          | lzo      | f       | 0       | f       | none       | not null  | t
 l_linestatus    | character(1)          | lzo      | f       | 0       | f       | none       | not null  | t
 l_shipdate      | date                  | delta    | f       | 1       | f       | none       | not null  | t
 l_commitdate    | date                  | delta    | f       | 0       | f       | none       | not null  | t
 l_receiptdate   | date                  | delta    | f       | 0       | f       | none       | not null  | t
 l_shipinstruct  | character(25)         | bytedict | f       | 0       | f       | none       | not null  | t
 l_shipmode      | character(10)         | bytedict | f       | 0       | f       | none       | not null  | t
 l_comment       | character varying(44) | text255  | f       | 0       | f       | none       | not null  | t
Has OIDs: yes
```

##Queries

Running the queries against the two tables we can see performance fluctuating between `280.088 ms` for the table encoded with delta, and `2706.043 ms` for the table encoded with runlength:

```sql
dev=# SELECT * FROM exercise_10.query_1;
 l_returnflag | l_linestatus |   sum_qty   | sum_base_price | sum_disc_price |   sum_charge   | avg_qty | avg_price | avg_disc | count_order
--------------+--------------+-------------+----------------+----------------+----------------+---------+-----------+----------+-------------
 N            | O            | 10703542.00 | 16047889918.44 | 15246680172.66 | 15857274595.97 |   25.51 |  38253.34 |     0.04 |      419516
(1 row)

Time: 280.088 ms
dev=# select pg_last_query_id();
 pg_last_query_id
------------------
           320310
(1 row)

Time: 1.299 ms
dev=# SELECT * FROM exercise_10.query_2;
 l_returnflag | l_linestatus |   sum_qty   | sum_base_price | sum_disc_price |   sum_charge   | avg_qty | avg_price | avg_disc | count_order
--------------+--------------+-------------+----------------+----------------+----------------+---------+-----------+----------+-------------
 N            | O            | 10703542.00 | 16047889918.44 | 15246680172.66 | 15857274595.97 |   25.51 |  38253.34 |     0.04 |      419516
(1 row)

Time: 2706.043 ms
dev=# select pg_last_query_id();
 pg_last_query_id
------------------
           320312
(1 row)
```

Checking the physical execution plan for these two queries we can see the only deviation being the `rows_pre_filter` amount and the `avgtime` duration.

```sql
dev=# select query,seg,step,avgtime,rows,rows_pre_filter,bytes,label,is_rrscan from svl_query_summary where query in (320310,320312) order by stm,seg,step,query;
 query  | seg | step | avgtime |  rows  | rows_pre_filter |  bytes   |                   label                   | is_rrscan
--------+-----+------+---------+--------+-----------------+----------+-------------------------------------------+-----------
 320310 |   0 |    0 |  201479 | 419516 |         6290562 | 19297736 | scan   tbl=108315 name=lineitem           | t
 320312 |   0 |    0 | 2580793 | 419516 |       280234000 | 19297736 | scan   tbl=108338 name=lineitem           | t
 320310 |   0 |    1 |  201479 | 419516 |               0 |        0 | project                                   | f
 320312 |   0 |    1 | 2580793 | 419516 |               0 |        0 | project                                   | f
 320310 |   0 |    2 |  201479 | 419516 |               0 |        0 | project                                   | f
 320312 |   0 |    2 | 2580793 | 419516 |               0 |        0 | project                                   | f
 320310 |   0 |    3 |  201479 |      6 |               0 |      960 | aggr   tbl=263                            | f
 320312 |   0 |    3 | 2580793 |      6 |               0 |      960 | aggr   tbl=263                            | f
 320310 |   0 |    4 |  201479 |      6 |               0 |        0 | dist                                      | f
 320312 |   0 |    4 | 2580793 |      6 |               0 |        0 | dist                                      | f
 320310 |   1 |    0 |  226696 |      6 |               0 |      960 | scan   tbl=105575 name=Internal Worktable | f
 320312 |   1 |    0 | 2656280 |      6 |               0 |      960 | scan   tbl=105579 name=Internal Worktable | f
 320310 |   1 |    1 |  226696 |      1 |               0 |      160 | aggr   tbl=266                            | f
 320312 |   1 |    1 | 2656280 |      1 |               0 |      160 | aggr   tbl=266                            | f
 320310 |   1 |    2 |  226696 |      1 |               0 |        0 | project                                   | f
 320312 |   1 |    2 | 2656280 |      1 |               0 |        0 | project                                   | f
 320310 |   1 |    3 |  226696 |      1 |               0 |        0 | project                                   | f
 320312 |   1 |    3 | 2656280 |      1 |               0 |        0 | project                                   | f
 320310 |   1 |    4 |  226696 |      1 |               0 |      128 | sort   tbl=269                            | f
 320312 |   1 |    4 | 2656280 |      1 |               0 |      128 | sort   tbl=269                            | f
 320310 |   1 |    5 |  226696 |      0 |               0 |        0 | merge                                     | f
 320312 |   1 |    5 | 2656280 |      0 |               0 |        0 | merge                                     | f
 320310 |   1 |    6 |  226696 |      0 |               0 |        0 | aggr   tbl=271                            | f
 320312 |   1 |    6 | 2656280 |      0 |               0 |        0 | aggr   tbl=271                            | f
 320310 |   1 |    7 |  226696 |      0 |               0 |        0 | project                                   | f
 320312 |   1 |    7 | 2656280 |      0 |               0 |        0 | project                                   | f
 320310 |   2 |    0 |      53 |      1 |               0 |      128 | scan   tbl=269 name=Internal Worktable    | f
 320312 |   2 |    0 |      65 |      1 |               0 |      128 | scan   tbl=269 name=Internal Worktable    | f
 320310 |   2 |    1 |      53 |      1 |               0 |        0 | return                                    | f
 320312 |   2 |    1 |      65 |      1 |               0 |        0 | return                                    | f
 320310 |   3 |    0 |      91 |      0 |               0 |        0 | merge                                     | f
 320312 |   3 |    0 |      90 |      0 |               0 |        0 | merge                                     | f
 320310 |   3 |    1 |      91 |      1 |               0 |        0 | project                                   | f
 320312 |   3 |    1 |      90 |      1 |               0 |        0 | project                                   | f
 320310 |   3 |    2 |      91 |      1 |               0 |        0 | project                                   | f
 320312 |   3 |    2 |      90 |      1 |               0 |        0 | project                                   | f
 320310 |   3 |    3 |      91 |      0 |               0 |        0 | return                                    | f
 320312 |   3 |    3 |      90 |      0 |               0 |        0 | return                                    | f
(38 rows)
```

The column `rows_pre_filter` means how many rows were fetched from disk into memory, before being pruned away with the user defined query filter. How this translates to an increase to runtime is due to additional blocks being fetched which contain these rows. Here are metrics regarding the number of blocks fetched by each of the queries (for each column, and for each query):

```
 query  |  tbl   | col | blocks
--------+--------+-----+--------
 320310 | 108315 |   4 |   18
 320312 | 108338 |   4 |  282
 320310 | 108315 |   5 |   36
 320312 | 108338 |   5 | 1116
 320310 | 108315 |   6 |   18
 320312 | 108338 |   6 |  282
 320310 | 108315 |   7 |   18
 320312 | 108338 |   7 |  282
 320310 | 108315 |   8 |   12
 320312 | 108338 |   8 |   48
 320310 | 108315 |   9 |   12
 320312 | 108338 |   9 |   48
 320310 | 108315 |  10 |   12
 320312 | 108338 |  10 |   12
 320310 | 108315 |  16 |   12
 320312 | 108338 |  16 |   24
(16 rows)

 query  | blocks
--------+------
 320310 |  138
 320312 | 2094
(2 rows)
```

##Explanation

To understand why `rows_pre_filter` increases significantly when we query the table with runlength encoding we need to dive deeper into the storage details of the column data blocks. The `l_shipdate` column is enumerated as 10 in the system catalog for these tables. Due to the data profile in `l_shipdate`, runlength encoding compresses the column extremely well. We can see that runlength encoding reduces the size of the column from 576MB with an average of 1041732 values per block, to 12MB with an average of 50,003,158 values per block.

```sql
dev=# select tbl,col,count(*),avg(num_values) from stv_blocklist where tbl in (108338,108315) and col = 10 and num_values > 0 group by 1,2 order by 1,2;
  tbl   | col | count |   avg
--------+-----+-------+----------
 108315 |  10 |   576 |  1041732
 108338 |  10 |    12 | 50003158
(2 rows)
```

Checking the actual number of rows which meet that filter criteria in the VIEW, we see it aligns with what was reported in the `rows` column of the `svl_query_summary` results above.

```sql
dev=# select count(*) from lineitem where lineitem.l_shipdate <= ('1998-12-01'::date - '66 days'::interval)::date AND lineitem.l_shipdate >= ('1998-12-01'::date - '68 days'::interval)::date;
 count
--------
 419516
(1 row)
```

Now, taking those rows to create a new table, we can get the min/max values from STV_BLOCKLIST.

```
dev=# create temp table l_shipdate_minmax as select l_shipdate from lineitem where lineitem.l_shipdate <= ('1998-12-01'::date - '66 days'::interval)::date AND lineitem.l_shipdate >= ('1998-12-01'::date - '68 days'::interval)::date;
dev=# select 'l_shipdate_minmax'::regclass::oid;
  oid
--------
 179739
(1 row)
dev=# select distinct minvalue, maxvalue from stv_blocklist where tbl=179739 and col=0;
 minvalue | maxvalue
----------+----------
     -464 |     -462
(1 row)
```

With these min/max values lets identify which blocks result in a hit for both tables:

```sql
dev=# select slice,tbl,col,blocknum,num_values,minvalue,maxvalue from stv_blocklist where tbl in (108338,108315) and col = 10 and (-464 between minvalue and maxvalue or -463 between minvalue and maxvalue or -462 between minvalue and maxvalue)   order by tbl,col,slice,blocknum;
 slice |  tbl   | col | blocknum | num_values | minvalue | maxvalue
-------+--------+-----+----------+------------+----------+----------
     0 | 108315 |  10 |       94 |    1048427 |     -487 |     -444
     1 | 108315 |  10 |       94 |    1048427 |     -487 |     -443
     2 | 108315 |  10 |       94 |    1048427 |     -487 |     -443
     3 | 108315 |  10 |       94 |    1048427 |     -487 |     -444
     4 | 108315 |  10 |       94 |    1048427 |     -487 |     -444
     5 | 108315 |  10 |       94 |    1048427 |     -487 |     -444
     0 | 108338 |  10 |        1 |   46713432 |    -1579 |     -396
     1 | 108338 |  10 |        1 |   46698495 |    -1579 |     -396
     2 | 108338 |  10 |        1 |   46690394 |    -1579 |     -396
     3 | 108338 |  10 |        1 |   46713344 |    -1579 |     -396
     4 | 108338 |  10 |        1 |   46703067 |    -1579 |     -396
     5 | 108338 |  10 |        1 |   46715268 |    -1579 |     -396
(12 rows)
```

Each query would leverage the zone maps to identify these 6 blocks to be fetched. At this point the query scan operation would know which blocks could potentially have the data that it needs, but since only the minimum and maximum values are available to them its not possible to be certain how many relevant rows are within these blocks. Because we don't know, we need to check - so we fetch the entire blocks to filter the data based on the filter we . 

This results in `6 * 1048427 = 6290562` rows being fetched for query 320310, and `6 * 46698495 = 280190970` rows being fetched for query 320312. These numbers correlate directly to what we saw in the output from `svl_query_summary.rows_pre_filter`.

##Translated to Block I/O?

In both cases we are still only fetching 6 blocks for this column, so there shouldn't be a significant performance degradation by that fact alone. This is where early materialization comes into play, and is responsible for the additional block I/O between queries. Checking the VIEW definition SQL we also see that these columns are necessary for the query to complete:

```sql
-- l_returnflag
-- l_linestatus
-- l_quantity
-- l_extendedprice
-- l_discount
-- l_tax
 SELECT 
  lineitem.l_returnflag, 
  lineitem.l_linestatus, 
  sum(lineitem.l_quantity) AS sum_qty, 
  sum(lineitem.l_extendedprice) AS sum_base_price, 
  sum(lineitem.l_extendedprice * (1::numeric::numeric(18,0) - lineitem.l_discount))::numeric(38,2) AS sum_disc_price, sum(lineitem.l_extendedprice::numeric(38,2) * (1::numeric::numeric(18,0) - lineitem.l_discount) * (1::numeric::numeric(18,0) + lineitem.l_tax))::numeric(38,2) AS sum_charge, avg(lineitem.l_quantity) AS avg_qty, avg(lineitem.l_extendedprice) AS avg_price, avg(lineitem.l_discount) AS avg_disc, count(*) AS count_order
   FROM exercise_10.lineitem
  WHERE lineitem.l_shipdate <= ('1998-12-01'::date - '66 days'::interval)::date AND lineitem.l_shipdate >= ('1998-12-01'::date - '68 days'::interval)::date
  GROUP BY lineitem.l_returnflag, lineitem.l_linestatus
  ORDER BY lineitem.l_returnflag, lineitem.l_linestatus;
```

These 6 additional columns correlate to col positions as shown below:

```sql
dev=# select tbl,col,count(*),avg(num_values) from stv_blocklist where tbl in (108338,108315) and num_values>0 group by 1,2 order by 1,2;
  tbl   | col | count |   avg
--------+-----+-------+----------
 108315 |   0 |  2364 |   253823
 108315 |   1 |  2364 |   253823
 108315 |   2 |  2292 |   261796
 108315 |   3 |   576 |  1041732
 108315 |   4 |   576 |  1041732 -- l_quantity
 108315 |   5 |  2364 |   253823 -- l_extendedprice
 108315 |   6 |   576 |  1041732 -- l_discount
 108315 |   7 |   576 |  1041732 -- l_tax
 108315 |   8 |   192 |  3125197 -- l_returnflag
 108315 |   9 |    78 |  7692793 -- l_linestatus
 108315 |  10 |   576 |  1041732
 108315 |  11 |   606 |   990161
 108315 |  12 |   576 |  1041732
 108315 |  13 |   576 |  1041732
 108315 |  14 |   576 |  1041732
 108315 |  15 |  7083 |    84715
 108315 |  16 |    24 | 25001579
 108315 |  17 |    24 | 25001579
 108315 |  18 |  4584 |   130898
 108338 |   0 |  2364 |   253823
 108338 |   1 |  2364 |   253823
 108338 |   2 |  2292 |   261796
 108338 |   3 |   576 |  1041732
 108338 |   4 |   576 |  1041732 -- l_quantity
 108338 |   5 |  2364 |   253823 -- l_extendedprice
 108338 |   6 |   576 |  1041732 -- l_discount
 108338 |   7 |   576 |  1041732 -- l_tax
 108338 |   8 |   192 |  3125197 -- l_returnflag
 108338 |   9 |    78 |  7692793 -- l_linestatus
 108338 |  10 |    12 | 50003158
 108338 |  11 |   606 |   990161
 108338 |  12 |   576 |  1041732
 108338 |  13 |   576 |  1041732
 108338 |  14 |   576 |  1041732
 108338 |  15 |  7083 |    84715
 108338 |  16 |    24 | 25001579
 108338 |  17 |    24 | 25001579
 108338 |  18 |  4584 |   130898
(38 rows)
```

So now we have everything needed to determine how many blocks we need to fetch in order to process this query using an early materialization strategy. For the query which scans `tbl=108315`, we have one block on each slice at position 94 with each block in this blockchain having an average of 1048427 values.

We can calculate the offset for the first value we fetched from block 94 in col 10 with (93*1048427+1 = 97503712) and from that position we know we need to fetch another 1048427 values, so our range is 97503712 -> 98552139. This range is what is critical, since within this range we know that after filtering we'll only be matching 419516 values. Having a range only 2x of what we are matching is a pretty good start.

To calculate how many blocks this requires to fetch for another column on this table, I'll consider `l_extendedprice`. With and average of 253823 values per block, to get to the relevant offset of 97503712 we can safely skip `(97503711 / 253823 ) = 384` blocks. Once at the offset we need to fetch enough blocks to match the range of 1048427 values, so about 4 blocks, on each slice. Continuing this process for each of the columns allows us to estimate how many blocks are needing to be fetched for the entire query.

Now lets see how that changes with an exceptionally compressed sortkey. Instead of having a range of 97503712 -> 98552139, the values we need are in the very first block. Unfortunately this block contains 46713432 values, so our range is 1-46713432. The offset for that same `l_extendedprice` column is calculated as (46713432 / 253823) = 184 blocks per slice.

##Mitigation

The trick to mitigating this issue is that when leveraging queries which perform rrscans on a column which is ordered either by a sortkey, or naturally with the data ingestion strategy, then we should seek to reduce the number of values per block for that column such that it is inline with other blocks' average number of values, for columns relevant to the same query. This won't result in a significant impact to fetch time against that one column, since we are already leveraging the zonemaps to effectively prune blocks, but has a large impact reducing the size of the range needing to be matched in other relevant columns. 

To help, there is a column in `svv_table_info` which measures `sortkey1_skew`, that is, a ratio of the size of the sortkey column relative to the largest column in the table. The idea for matching these two queries is based on assumptions that the largest column is being scanned in this query pattern, and that the sortkey column is well compressed (they typically are, due to common data ordering). 

*Note: Manually calculating column size skew for your own query patterns, and the columns they access, will always be more accurate than this general `svv_table_info.sortkey1_skew` metric. Since the sortkey column isn't always necessarily the most well compressed column, the column used for rrscans, and the non-rrscan columns may not necessarily ever be the largest column in a given table.*
