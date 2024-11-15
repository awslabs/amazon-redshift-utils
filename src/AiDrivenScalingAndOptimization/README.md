# Instructions on how to use the demo notebooks for Amazon Redshift Serverless AI-driven scaling and optimization

In the repository you will find two demo notebooks demonstrating two key behaviors of the Serverless AI-driven scaling and optimization algorithms.
* Scaling Notebook: In this notebook we demonstrate how the AI algorithms infer various attributes about a SQL query and scale the compute when a same/similar long query is encountered again on the cluster, resulting into a significant performance improvement.
* Resizing Notebook: In this notebook we demonstrate how the AI algorithms infer various attributes about the workload running on the Redshift cluster and scales the cluster size up/down depending on the workload and the price-performance target chosen by the cluster via the performance slider.

## Scaling Notebook
In this notebook we demonstrate how the AI algorithms infer various attributes about a SQL query and scale the compute when a same/similar long query is encountered again on the cluster, resulting into a significant performance improvement.

Steps to follow:
* Step 1: Create a new Redshift Serverless cluster from the AWS console.
* Step 2: Choose the performance slider setting as 100 from the console. This setting optimizes for performance.
* Step 3: Upload the notebook in Redshift Query Editor V2.
* Step 4: Open the notebook and connect it to the cluster you just created in Step 1.
* Step 5: Create a new database or connect to an existing database of your choice.
* Step 6: Run the cells in the notebook. The cells will create the necessary tables, load them with data, and run a heavy/long query on the cluster which will take a very long time to finish execution (1.5 to 2.5 hours). It will then calculate the cost of running the query. 
* Step 7: The next set of cells will run the same query again with result_cache disabled. This time Amazon Redshift Serverless AI-driven scaling and optimization will scale the query resulting it in a much shorter execution time (0.5 to 1 hour). The last cell will then again calculate the cost of running the query and you the difference in time taken and compute utilized can be clearly observed.

## Resizing Notebook
In this notebook we demonstrate how the AI algorithms infer various attributes about the workload running on the Redshift cluster and scales the cluster size up/down depending on the workload and the price-performance target chosen by the cluster via the performance slider.

Steps to follow:
* Step 1: Create a new Redshift Serverless cluster from the AWS console.
* Step 2: Choose the performance slider setting as 1 from the console. This setting optimizes for cost. Note that you will start with the default 128RPUs.
* Step 3: Upload the notebook in Redshift Query Editor V2.
* Step 4: Open the notebook and connect it to the cluster you just created in Step 1.
* Step 5: Create a new database or connect to an existing database of your choice.
* Step 6: Run the cells in the notebook. The first few cells will simply create the necessary tables and load them with data.
* Step 7: To mimic a long multi-hour workload we have provided a procedure which creates a workload that runs 5000 queries every hour. You can use the "Schedule Query" feature in the Redshift Query Editor to schedule this query for next 1 day at an hourly frequency. This feature is available if you paste this query independently into a new query editor window (not a new notebook). You should see a "Schedule" button on top right to configure/schedule this query.
* Step 8: Once successfully scheduled, you can return to the notebook the next day and just run the last cell which shows the compute capacity utilized by cluster over time. You should notice a decline in the compute capacity over time. More specifically, you should see that cluster starts from 128 RPUs which is the default. Over time the AI-based scaling and optimization will learn from the workload and perform necessary optimizations to favor "cost" as we have our slider set to "Optimizes for cost" resulting in the compute capacity to go down to a 64RPUs to 32RPUs.
