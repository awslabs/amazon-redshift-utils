CREATE OR REPLACE PROCEDURE refresh_account_change_summary()
AS $$
    BEGIN
    -- Create staging table with a column for type of operation
    CREATE TABLE IF NOT EXISTS "public"."stage_pgbench_accounts" (LIKE "public"."pgbench_accounts");
    ALTER TABLE "public"."stage_pgbench_accounts" ADD COLUMN op VARCHAR(8);
    -- Save the last version of the previous day records into the staging table
    INSERT INTO "public"."stage_pgbench_accounts" (changets, aid, bid, abalance, filler, op)
    SELECT changets, aid, bid, abalance, filler, op
    FROM (
        SELECT *, row_number() over (partition by aid order by changets desc) AS lastchange
        FROM "cdc_database"."raw_cdc_pgbench_accounts"
        WHERE partition_0>=to_char(DATE(DATEADD(DAY, -1, GETDATE())), 'YYYYMMDD')
    )
    WHERE lastchange=1;
    -- Clean changed records from the original table
    DELETE FROM "public"."pgbench_accounts" USING "public"."stage_pgbench_accounts"
    WHERE "public"."pgbench_accounts"."aid" = "public"."stage_pgbench_accounts"."aid"
    AND "public"."pgbench_accounts"."changets" < "public"."stage_pgbench_accounts"."changets";
    -- Clean pre-existing records from the staging table
    DELETE FROM "public"."stage_pgbench_accounts" USING "public"."pgbench_accounts"
    WHERE "public"."pgbench_accounts"."aid" = "public"."stage_pgbench_accounts"."aid"
    AND"public"."pgbench_accounts"."changets" >= "public"."stage_pgbench_accounts"."changets";
    -- Update the destination table with records from the staging table
    INSERT INTO "public"."pgbench_accounts"
    SELECT changets, aid, bid, abalance, filler
    FROM (
        SELECT * FROM "public"."stage_pgbench_accounts" WHERE op='I' OR op='U'
    );
    -- Refresh the materialized view
    REFRESH MATERIALIZED VIEW "public"."weekly_account_change_summary_mv";
    -- Delete the staging table
    DROP TABLE "public"."stage_pgbench_accounts";
    END;
$$ LANGUAGE plpgsql;
