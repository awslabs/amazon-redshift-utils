The scripts in here are to support the blog given below. 
Blog Name : How to evaluate the benefits of AQUA for your Amazon Redshift workloads
Link to blog:
https://quip-amazon.com/EmveAhrwjUdr/How-to-evaluate-the-benefits-of-AQUA-for-your-Amazon-Redshift-workloads

How to install/setup:
Download the scripts from GitHub and copy to a machine from where you would like to execute the scripts. The machine should have proper network communication enabled to reach your Amazon Redshift clusters as described in the accompanying blogpost.
Pre-requisites:
User for DB should have access read, write access to all user tables and full read access to system
catalog tables
psql client libraries are in the path to execute scripts mentioned below


Script Short Description and Outcome:
 aqua_capture_query.sh -- Capture aqua-eligible queries from your cluster and save the output to aqua_eligible_queries.sql
aqua_execute_query.sh -- Execute queries captured in capture.sql on the specified cluster with/without AQUA. Saves script execution start and  end times to workload_datetime.txt
 aqua_perf_compare.sh -- Compare performance of captured aqua-eligible queries in Amazon Redshift with/without AQUA. It generates a CSV file named aqua_benefit.csv and also displays a performance comparison information in the terminal.
execute_test_queries.sh --Execute benchmarking queries against amazon sentiments test data with/without AQUA.
load_amazon_sentiments_data.sql -- Load amazon sentiments data from public S3 bucket into test cluster.

Mandatory Parameters Details:
Input for  aqua_capture_query.sh,  -h cluster hostname/IP, -d DatabaseName, -s Start datetime of workload -e End datetime of workload, -p port.
Input for   aqua_execute_query.sh, -h cluster hostname/IP, -d DatabaseName -p port. 
Input for  aqua_perf_compare.sh, -h cluster hostname/IP, -d DatabaseName -p port.

Sample Execution :
./aqua_capture_query.sh -h 10.x.x.xxx -p 5439 -d TestDB -U test_user -s "2021-09-06 00:00:00" -e "2021-09-09 23:00:00"
./aqua_execute_query.sh -h 10.x.x.xxx -p 5439 -d TestDB -U test_user
./aqua_perf_compare.sh -h 10.x.x.xxx -p 5439 -d TestDB -U test_user
./execute_test_queries.sh h 10.x.x.xxx -p 5439 -d TestDB -U test_user

