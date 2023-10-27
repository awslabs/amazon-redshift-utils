# Cloud Data Warehouse Benchmark Derived from TPC-DS 2.13

## Overview

Amazon Redshift is a fast, scalable data warehouse that makes it simple and cost-effective to analyze all your data across your data warehouse and data lake. Redshift delivers faster performance than other data warehouses by using machine learning, massively parallel query execution, and columnar storage on high-performance disk. We continuously improve the performance of the Redshift service. One workload that we use is the Cloud DW benchmark derived from TPC-DS (http://www.tpc.org/tpcds/default.asp).

_Note: The TPC Benchmark and TPC-DS are trademarks of the Transaction Processing Performance Council (http://www.tpc.org). The Cloud DW benchmark is derived from the TPC-DS Benchmark and as such is not comparable to published TPC-DS results._

The intent of this benchmark is to simulate a set of basic scenarios to answer fundamental business questions and report business outcomes relevant to many industry sectors. 

>
“TPC-DS models the decision support functions of a retail product supplier. The supporting schema contains vital business information, such as customer, order, and product data.
In order to address the enormous range of query types and user behaviors encountered by a decision support system, TPC-DS utilizes a generalized query model. This model allows the benchmark to capture important aspects of the interactive, iterative nature of on-line analytical processing (OLAP) queries, the longer-running complex queries of data mining and knowledge discovery, and the more planned behavior of well known report queries.”

The Cloud DW benchmark consists of the 99 queries from TPC-DS. These have a wide variation of complexity, amount of data scanned, answer set size and elapsed time. Each query asks a business question and includes the corresponding query to answer the question. We have generated TPC-DS datasets and corresponding queries using the official TPC-DS kit from tpc.org. We have also provided sample DDL for each of the tables in the Cloud DW benchmark. This gives you an easy and complete way of setting up your own Cloud DW benchmark on Redshift and running the same queries that we use in our own testing.  While the queries we provide use the default random seed as provided by TPC-DS, you can use the tools in the TPC-DS Benchmark Kit to generate different versions of these queries that have different parameter values.

## Repository Structure

This repository contains a directory for each data scale that we have generated.  For example, the `3TB` directory includes the queries, table definitions, and load SQL for the 3 Terabyte TPC-DS dataset.  The data itself is stored in Amazon S3.

The files in each data scale directory are as follows:
* `ddl.sql`:  This SQL file creates the TPC-DS tables and then loads the data via COPY from S3. Prior to executing, you will need to replace the strings `<USER_ACCESS_KEY_ID>` and `<USER_SECRET_ACCESS_KEY>` with any valid S3 credentials.  The file also executes `SELECT COUNT(*)` against all tables at the end to show that all data was loaded.
* `queries/query_0.sql`:  This file contains the 99 queries that are part of the TPC-DS "Power Run".  The "Power Run" is a single-concurrency execution of the official TPC-DS queries.  Note that 4 of the TPC-DS "queries" have 2 parts, so executing the file will result in 103 SQL queries being executed.


## Instructions For Use

### Power Run

To execute the TPC-DS Power Run at a particular data scale (e.g. 3TB), perform the following steps:
1. Create a new database to load the dataset into (e.g."CREATE DATABASE tpcds_3tb;")

2. Connect to the database you created and execute `ddl.sql`.  This may take several hours, depending on the data scale and data warehouse size.

3. `ddl.sql` will execute `SELECT COUNT(*)` at the end against each table.  Verify that the resulting counts match the values at the end of `ddl.sql`.

4. Execute `query_0.sql` three times and record the best run time.

Note that the S3 data is located in US-EAST-1 so you may get faster load times if your test cluster is located in that region.
