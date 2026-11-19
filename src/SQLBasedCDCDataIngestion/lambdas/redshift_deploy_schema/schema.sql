CREATE TABLE IF NOT EXISTS public.pgbench_branches(
    changets VARCHAR(26) ENCODE zstd,
    bid INTEGER ENCODE az64,
    bbalance INTEGER ENCODE az64,
    filler VARCHAR(100) ENCODE zstd,
    primary key(bid)
) DISTSTYLE ALL
sortkey(bid);
CREATE TABLE IF NOT EXISTS public.pgbench_tellers(
    changets VARCHAR(26) ENCODE zstd,
    tid INTEGER ENCODE az64,
    bid INTEGER ENCODE az64,
    tbalance INTEGER ENCODE az64,
    filler VARCHAR(100) ENCODE zstd,
    primary key(tid), 
    foreign key(bid) references public.pgbench_branches(bid)
) DISTSTYLE ALL
sortkey(bid);
CREATE TABLE IF NOT EXISTS public.pgbench_accounts(
    changets VARCHAR(26) ENCODE zstd,
    aid INTEGER ENCODE az64,
    bid INTEGER ENCODE az64,
    abalance INTEGER ENCODE az64,
    filler VARCHAR(100) ENCODE zstd,
    primary key(aid),
    foreign key(bid) references public.pgbench_branches(bid)
    )
distkey(aid)
sortkey(bid);
CREATE TABLE IF NOT EXISTS public.pgbench_history(
    changets VARCHAR(26) ENCODE zstd,
    tid INTEGER ENCODE az64,
    bid INTEGER ENCODE az64,
    aid INTEGER ENCODE az64,
    "delta" INTEGER ENCODE az64,
    mtime TIMESTAMP WITHOUT TIME ZONE ENCODE zstd,
    filler VARCHAR(100) ENCODE zstd,
    foreign key(bid) references public.pgbench_branches(bid),
    foreign key(aid) references public.pgbench_accounts(aid),
    foreign key(tid) references public.pgbench_tellers(tid)
   )
distkey(aid)
sortkey(mtime);
CREATE MATERIALIZED VIEW weekly_account_change_summary_mv
distkey(aid)
sortkey(reporting_week) as 
select 
    h.aid,
    sum("h"."delta") as "weekly_change",
    DATE_TRUNC('week',h.mtime) as "reporting_week",
    a.filler 
from public.pgbench_history h  
join public.pgbench_accounts a on h.aid=a.aid 
group by h.aid,DATE_TRUNC('week',h.mtime ),a.filler