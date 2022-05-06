# Synthetic Mixed Read/Write Benchmark Derived from TPC-H

Amazon Redshift is a fast, scalable data warehouse that makes it simple and cost-effective to analyze all your data across your data warehouse and data lake. Redshift delivers faster performance than other data warehouses by using machine learning, massively parallel query execution, and columnar storage on high-performance disk. 
We continuously improve the performance of the Redshift service. One workload that we use is the Cloud Data Warehouse Mixed Read/Write Benchmark Derived from TPC-H (http://www.tpc.org/tpch/default.asp).

Note: The TPC Benchmark and TPC-H are trademarks of the Transaction Processing Performance Council (http://www.tpc.org). The Cloud Data Warehouse Mixed Read/Write Benchmark is derived from the TPC-H Benchmark and as such is not comparable to published TPC-H results.

    "The TPC Benchmark (TPC-H) is a decision support benchmark. It consists of a suite of business oriented ad-hoc queries and concurrent data modifications. The queries and the data populating the database have been chosen to have broad industry-wide relevance while maintaining a sufficient degree of ease of implementation. 
	This benchmark illustrates decision support systems that

        Examine large volumes of data;
        Execute queries with a high degree of complexity;
        Give answers to critical business questions."

The main intention to create this benchmark is to mimic a real world mixed workload scenario, where some applications continuously read from datamart set of tables, Analysts run adhoc queries throughout the day, ingestions are happening against the larger data warehouse set of tables on a periodic manner. Also there are some pre-defined set of reports which run on a schedule. In this derived benchmark, we have broken down these 22 queries into 3 categories based on the run timings. The short ones we marked as DASHBOARD queries. The longest ones marked as DATASCIENCE and medium ones are categorized as REPORT. The corresponding queries would also be submitted with different speed (configurable) and concurrency. All these queries are submitted in an infinite loop with a finite wait between them. The wait time is introduced to mimic a scenario where data analysts process the query output data locally for next 30 mins before submitting another query. The concurrency is to represent the number of concurrent users. In real world, different group of users also uses different data sets. For example, API calls the datamart tables for quick lookup whereas analysts generally work against the larger data warehouse set of tables. Common practice that Data ingestion happens against the larger dataset frequently.

With the attached scripts, DASHBOARD queries are submitted as 16 concurrent users with 2 secs wait, REPORT queries in 6 parallel streams with 15 mins wait, 4 parallel streams of DATASCIENCE queries with 30 mins wait. We have the mix dataset of both TPC-H 100GB and TPC-H 3TB. The smaller TPC-H 100GB represents a datamart and the larger TPC-H 3TB is the main data warehouse. REPORT and DATASCIENCE queries read from the larger TPC-H 3TB dataset, against which the COPY loads are going in every hour. For the copy we load the TPC-H 100GB data files on top of the TPC-H 3TB data tables.

Here are the steps to use this benchmark:
## 1. Cluster Set up: 
    Create 2 Redshift clusters with different configurations, one as a baseline and one as target


* We have used these WLM configurations to assess the performance between Manual WLM and Auto WLM:


### Manual Configuration	

Queues/Query Groups	|Memory %	|Max Concurrency	|Concurrency Scaling	|Priority		
--------------------|--------|------------------|-----------------------|---------
Dashboard	|24|	5	|Off	|NA	
Report	|25	|6	|Off	|NA		
DataScience	|25	|4	|Off	|NA	
COPY	|25	|3	|Off	|NA	
Default	|1	|1	|Off	|NA	

### Automatic Configuration

Queues/Query Groups	|Memory %	|Max Concurrency	|Concurrency Scaling	|Priority
------------------------|---------------|-----------------------|-----------------------|------
Dashboard	|Auto	|Auto	|Off	|Normal
Report	|Auto	|Auto	|Off	|Normal
DataScience	|Auto	|Auto	|Off	|Normal
COPY	|Auto	|Auto	|Off	|Normal
Default	|Auto	|Auto	|Off	|Normal


## 2. Metadata Set up: 
Run these once against each cluster... This will set up some users, tables, metadata for the workload run  
```
    export PGPASSWORD=<masteruserpassword>
    psql -h cluster1.ck0e91yaz.us-east-1.redshift.amazonaws.com -p \<portnumber\> -d \<database\> -U \<masteruser\> -f tpc-h-sqls.sql > tpc-h-sqls.sql.manua.log
    psql -h cluster2.ck0e91yaz.us-east-1.redshift.amazonaws.com -p \<portnumber\> -d \<database\> -U \<masteruser\> -f tpc-h-sqls.sql > tpc-h-sqls.sql-auto.log
```
    With sql, you can check if the tables are populated properly. select sql_type  ,  tpch_sql_number from tpc_h_sqls order by 1,2; Should return 28 rows.

## 3. Initial Data load: 
Run these once against each cluster.
Please note, you have to provide the access key and id for the COPY commands. 
These steps would take time depending on the instance type. We recommend that you run them in the background.

    export PGPASSWORD=<masteruserpassword>
    nohup psql -h cluster1.ck0e91yaz.us-east-1.redshift.amazonaws.com -p \<portnumber\> -d \<database\> -U \<masteruser\> -f tpc-h-100g-create-tables.sql > tpc-h-100g-create-tables.sql.01.out & 
    nohup psql -h cluster2.ck0e91yaz.us-east-1.redshift.amazonaws.com -p \<portnumber\> -d \<database\> -U \<masteruser\> -f tpc-h-100g-create-tables.sql > tpc-h-100g-create-tables.sql.02.out &
    nohup psql -h cluster1.ck0e91yaz.us-east-1.redshift.amazonaws.com -p \<portnumber\> -d \<database\> -U \<masteruser\> -f tpc-h-3T-create-tables.sql > tpc-h-3T-create-tables.sql.01.out &
    nohup psql -h cluster2.ck0e91yaz.us-east-1.redshift.amazonaws.com -p \<portnumber\> -d \<database\> -U \<masteruser\> -f tpc-h-3T-create-tables.sql > tpc-h-3T-create-tables.sql.02.out &

## 4. Workload Execution: 
This will start the workload in the background. Please note, you need python3.6 installed. You can run as many time as you want. To reset the data, start with step 2. 

    nohup ./run-all-bg-by-host.sh "cluster1.ck0e91r14yaz.us-east-1.redshift.amazonaws.com" "\<portnumber\>" "\<database\>" "\<masteruser\>" "\<masteruserpassword\>" "\<awskeyid\>" "\<awssecretkey\>" > run-all-bg-by-host.01.out &
    nohup ./run-all-bg-by-host.sh "cluster2.ck0e91r14yaz.us-east-1.redshift.amazonaws.com" "\<portnumber\>" "\<database\>" "<\masteruser\>" "\<masteruserpassword\>" "\<awskeyid\>" "\<awssecretkey\>" > run-all-bg-by-host.02.out &


## 5. Workload Stop: 
The following script can kill all the back ground jobs. Please use the proper os user name.
```
    for i in \`ps -fu ec2-user | grep "python3 ParallelExecute" | cut -d ' ' -f2\` ; do kill -9 "$i"; done
```

