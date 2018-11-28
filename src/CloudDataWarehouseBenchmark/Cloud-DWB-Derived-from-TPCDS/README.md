# Cloud Data Warehouse Benchmark Derived from TPC-DS

Amazon Redshift is a fast, scalable data warehouse that makes it simple and cost-effective to analyze all your data across your data warehouse and data lake. Redshift delivers faster performance than other data warehouses by using machine learning, massively parallel query execution, and columnar storage on high-performance disk. We continuously improve the performance of the Redshift service. One workload that we use is the Cloud DW benchmark derived from TPC-DS (http://www.tpc.org/tpcds/default.asp).

_Note: The TPC Benchmark and TPC-DS are trademarks of the Transaction Processing Performance Council (http://www.tpc.org). The Cloud DW benchmark is derived from the TPC-DS Benchmark and as such is not comparable to published TPC-DS results._

The intent of this benchmark is to simulate a set of basic scenarios to answer fundamental business questions and report business outcomes relevant to many industry sectors. 

>
“TPC-DS models the decision support functions of a retail product supplier. The supporting schema contains vital business information, such as customer, order, and product data.
In order to address the enormous range of query types and user behaviors encountered by a decision support system, TPC-DS utilizes a generalized query model. This model allows the benchmark to capture important aspects of the interactive, iterative nature of on-line analytical processing (OLAP) queries, the longer-running complex queries of data mining and knowledge discovery, and the more planned behavior of well known report queries.”

The Cloud DW benchmark consists of the same set of 99 queries as in TPC-DS. These have a wide variation of complexity, amount of data scanned, answer set size and elapsed time. Each query asks a business question and includes the corresponding query to answer the question. We have generated instances of the 99 queries for a given database scale,  3TB, and we have used the well defined default seed for the random number generator. We have also provided sample DDL for each of the tables in the Cloud DW benchmark. Similarly we have generated the input data files that you can use as a data source to populate the tables. This gives you an easy and complete way of setting up your own Cloud DW benchmark on Redshift and running the same queries that we use in our own testing. Alternatively, you can use the tools in the TPC-DS Benchmark Kit to generate many different versions of these queries that vary by parameter values.


