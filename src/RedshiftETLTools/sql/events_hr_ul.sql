select * from events where timestamp >= (select date_trunc(''hour'',max(timestamp)) from events)
