# Cloud Data Warehouse Benchmark Derived from TPC-H

Amazon Redshift is a fast, scalable data warehouse that makes it simple and cost-effective to analyze all your data across your data warehouse and data lake. Redshift delivers faster performance than other data warehouses by using machine learning, massively parallel query execution, and columnar storage on high-performance disk. We continuously improve the performance of the Redshift service. One workload that we use is the Cloud DW benchmark derived from TPC-H (http://www.tpc.org/tpch/default.asp).

_Note: The TPC Benchmark and TPC-H are trademarks of the Transaction Processing Performance Council (http://www.tpc.org). The Cloud DW benchmark is derived from the TPC-H Benchmark and as such is not comparable to published TPC-H results._

The intent of this benchmark is to simulate a set of basic scenarios to answer fundamental business questions and report business outcomes relevant to many industry sectors. 

>"The TPC Benchmark (TPC-H) is a decision support benchmark. It consists of a suite of business oriented ad-hoc queries and concurrent data modifications. The queries and the data populating the database have been chosen to have broad industry-wide relevance while maintaining a sufficient degree of ease of implementation. This benchmark illustrates decision support systems that 
>
>- Examine large volumes of data;
>- Execute queries with a high degree of complexity;
>- Give answers to critical business questions."

The Cloud DW benchmark derived from TPC-H consists of the same set of 22 queries as in TPC-H. These have a wide variation of complexity, amount of data scanned, answer set size and elapsed time. Each query asks a business question and includes the corresponding query to answer the question. We have generated instances of the 22 queries for a given database scale,  3TB, and we have used the well defined default seed for the random number generator. We have also provided sample DDL for each of the tables. Similarly we have generated the input data files that you can use as a data source to populate the tables. This gives you an easy and complete way of setting up your own Cloud DW benchmark on Redshift and running the same queries that we use in our own testing. Alternatively, you can use the tools in the TPC-H Benchmark Kit to generate many different versions of these queries that vary by parameter values.


