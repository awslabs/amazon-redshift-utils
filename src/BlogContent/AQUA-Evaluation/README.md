The scripts in here are to support the blog given below.
  
Blog name: How to evaluate the benefits of AQUA for your Amazon Redshift workloads

How to install/setup:
    Download the scripts from GitHub and copy to a machine from where you would like to execute the scripts. The machine should have proper network communication enabled to reach your Amazon Redshift clusters as described in the accompanying blogpost.

Pre-requisites:
    user for DB should have cluster access as a privileged superuser.
    psql client libraries should be in the path.


Script Short Description and Outcome:

    aqua_capture_query.sh -- captures aqua-eligible queries from your cluster and save the output to aqua_eligible_queries.sql
    aqua_execute_query.sh -- executes queries captured in capture.sql on the specified cluster with/without AQUA. Saves script execution start and end times to workload_datetime.txt
    aqua_perf_compare.sh -- compares performance of captured aqua-eligible queries with/without AQUA and generates a CSV file named aqua_benefit.csv and also displays performance comparison to the terminal.
    execute_test_queries.sh --executes benchmarking queries against amazon sentiments test data with/without AQUA.
    load_amazon_sentiments_data.sql --loads amazon sentiments data from public s3 bucket into test cluster.

Mandatory Parameters Details:

    aqua_capture_query.sh, -h cluster hostname/IP, -d databasedame -s "start datetime of workload" -e "end datetime of workload" -p port.
    aqua_execute_query.sh, -h cluster hostname/IP, -d databasename -p port. 
    aqua_perf_compare.sh, -h cluster hostname/IP, -d databasename -p port.

Sample Execution :

    ./aqua_capture_query.sh -h 10.x.x.xxx -p 5439 -d testDB -U test_user -s "2021-09-06 00:00:00" -e "2021-09-09 23:00:00"
    ./aqua_execute_query.sh -h 10.x.x.xxx -p 5439 -d testDB -U test_user
    ./aqua_perf_compare.sh -h 10.x.x.xxx -p 5439 -d testDB -U test_user
    ./execute_test_queries.sh h 10.x.x.xxx -p 5439 -d testDB -U test_user

