# Cloud Data Warehouse Benchmark Derived from TPC-H

## Overview

Amazon Redshift is a fast, scalable data warehouse that makes it simple and cost-effective to analyze all your data across your data warehouse and data lake. Redshift delivers faster performance than other data warehouses by using machine learning, massively parallel query execution, and columnar storage on high-performance disk. We continuously improve the performance of the Redshift service. One workload that we use is the Cloud DW benchmark derived from TPC-H (http://www.tpc.org/tpch/default.asp).

_Note: The TPC Benchmark and TPC-H are trademarks of the Transaction Processing Performance Council (http://www.tpc.org). The Cloud DW benchmark is derived from the TPC-H Benchmark and as such is not comparable to published TPC-H results._

The intent of this benchmark is to simulate a set of basic scenarios to answer fundamental business questions and report business outcomes relevant to many industry sectors. 

>"The TPC Benchmark (TPC-H) is a decision support benchmark. It consists of a suite of business oriented ad-hoc queries and concurrent data modifications. The queries and the data populating the database have been chosen to have broad industry-wide relevance while maintaining a sufficient degree of ease of implementation. This benchmark illustrates decision support systems that 
>
>- Examine large volumes of data;
>- Execute queries with a high degree of complexity;
>- Give answers to critical business questions."

The Cloud DW benchmark derived from TPC-H consists of the same set of 22 queries as in TPC-H. These have a wide variation of complexity, amount of data scanned, answer set size and elapsed time. Each query asks a business question and includes the corresponding query to answer the question. We have generated instances of the 22 queries for each database scale and we have used the default random seed for the random number generator. We have also provided sample DDL for each of the tables. Similarly we have generated the input data files that you can use as a data source to populate the tables. This gives you an easy and complete way of setting up your own Cloud DW benchmark on Redshift and running the same queries that we use in our own testing. Alternatively, you can use the tools in the TPC-H Benchmark Kit to generate many different versions of these queries that vary by parameter values.


## Repository Structure

This repository contains a directory for each data scale that we have generated.  For example, the `3TB` directory includes the queries, table definitions, and load SQL for the 3 Terabyte TPC-H dataset.  The data itself is stored in Amazon S3.

The files in each data scale directory are as follows:
* `ddl.sql`:  This SQL file creates the TPC-H tables and then loads the data via COPY from S3. Prior to executing, you will need to replace the strings `<USER_ACCESS_KEY_ID>` and `<USER_SECRET_ACCESS_KEY>` with any valid S3 credentials.  The file also executes `SELECT COUNT(*)` against all tables at the end to show that all data was loaded.
* `queries/query_0.sql`:  This file contains the 22 queries that are part of the TPC-H "Power Run".  The "Power Run" is a single-concurrency execution of the official TPC-H queries.  Note that 4 of the TPC-H "queries" have 2 parts, so executing the file will result in 103 SQL queries being executed.
* `queries/query_1.sql` through `queries/query_10.sql`:  These files contain the same 22 queries, but each in a different random order.  The random order is defined by the TPC-H specification.  These files are used for the TPC-H "Throughput Run". The "Throughput Run" is a multi-concurrency execution. So for example, to execute a 5-user Throughput Run, the files `query_1.sql` through `query_5.sql` should be executed at the same time against the database.


## Instructions For Use

### Power Run

To execute the TPC-H Power Run at a particular data scale (e.g. 3TB), perform the following steps:
1. Create a new database to load the dataset into (e.g."CREATE DATABASE tpch_3tb;")

2. Connect to the database you created and execute `ddl.sql`.  This may take several hours, depending on the data scale and data warehouse size.

3. Execute `query_0.sql` three times and record the best run time.

Note that the S3 data is located in US-EAST-1 so you may get faster load times if your test cluster is located in that region.

### Throughput Run

Once you have the data loaded and have executed the Power Run, you can then execute the Throughput Run using the same data:

4. Simultaneously execute the query files that correspond to the number of concurrent streams you want.  For example, to execute 2 concurrent streams, simultaneously execute `query_1.sql` and `query_2.sql`.  To execute 5 concurrent streams simultaneously execute `query_1.sql` through `query_5.sql`.  The Throughput Run time will be the time elapsed from the start of the first query for the streams to the end of the last query of any stream.
