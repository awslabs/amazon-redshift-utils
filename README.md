# Amazon Redshift Utilities

Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

    http://aws.amazon.com/asl/

Amazon Redshift is a fast, fully managed, petabyte-scale data warehouse solution 
that uses columnar storage to minimise IO, provide high data compression rates, 
and offer fast performance. This GitHub provides a collection of scripts and utilities
that will assist you in getting the best performance possible from Amazon Redshift.

# Admin Scripts

In the AdminScripts directory, you will find a collection of utilities for running
diagnostics on your Cluster, generating Schema DDL, and ...

# Column Encoding Utility

In order to get the best performance from your Redshift Database, you must ensure 
that database tables have the correct Column Encoding applied (http://docs.aws.amazon.com/redshift/latest/dg/t_Compressing_data_on_disk.html). 
Column Encoding specifies which algorithm is used to compress data within a column, 
and is chosen on the basis of the datatype, the unique number of discrete values 
in the column, and so on. When the COPY command (http://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html) 
is used to load data into a table, column encoding will be analyzed and applied by default. 
Other tables may be loaded via Extract/Load/Transform/Load (ELT) processes, and 
these tables may require having the column encoding updated at some point.

The Redshift Column Encoding Utility gives you the ability to apply optimal Column 
Encoding to an established Schema with data already loaded. When run, it will analyze 
an entire schema or individual tables. The ANALYZE COMPRESSION (http://docs.aws.amazon.com/redshift/latest/dg/r_ANALYZE_COMPRESSION.html) 
command is used to determine if any of the columns in the table require updating, 
and if so a script is generated to convert to the optimal structure.

----
Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

    http://aws.amazon.com/asl/
