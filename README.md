# Amazon Redshift Column Encoding Utility

Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

    http://aws.amazon.com/asl/

Amazon Redshift is a fast, fully managed, petabyte-scale data warehouse solution that uses columnar storage to minimise IO, provide high data compression rates, and offer fast performance. In order to get the best performance, you must ensure that database tables have the correct Column Encoding applied (http://docs.aws.amazon.com/redshift/latest/dg/t_Compressing_data_on_disk.html). When the COPY command (http://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html) is used to load data into a table, column encoding will be analyzed and applied by default. Other tables may be loaded via Extract/Load/Transform/Load (ELT) processes, and these tables may require having the column encoding updated at some point.

The Redshift Column Encoding Utility gives you a variety of options for applying optimal Column Encoding to an established Schema with data already loaded. When run, it will analyze the entire schema requested, or can be run on individual tables. The ANALYZE COMPRESSION (http://docs.aws.amazon.com/redshift/latest/dg/r_ANALYZE_COMPRESSION.html) command is used to determine if any of the columns in the table require updating, and if so a script is generated which will create new tables which have the correct column encoding applied, while retaining your distribution and sort keys. Your data will be migrated into the new table, and then you can choose to keep the old data or drop it. You can also do an in place migration using a single schema, or you can request that a new schema be built for the new tables, so that you can test before using the new structure.

Because this utility can make changes to your database live (using the ```--do-execute true``` option, it is highly recommended that you thoroughly test the utility against a dev/test system, and ensure that running the generated script will not adversely impact Cluster customers.

## Running the Column Encoding Utility

This utility was built and tested on Python 2.7x, but may work with other versions of Python. After cloning this Github project, you must ensure that you have installed the PsycoPG2 driver for Postgres (http://initd.org/psycopg). You can then run the column encoding utility by typing ```python analyze-schema-compression.py``` or ```./analyze-schema-compression.py```. This will generate the following Usage instructions:

```
Usage: analyze-schema-compression.py
       Generates a script to optimise Redshift column encodings on all tables in a schema

Arguments: --db             - The Database to Use
           --db-user        - The Database User to connect to
           --db-host        - The Cluster endpoint
           --db-port        - The Cluster endpoint port (default 5439)
           --analyze-schema - The Schema to be Analyzed (default public)
           --analyze-table  - A specific table to be Analyzed, if --analyze-schema is not desired
           --target-schema  - Name of a Schema into which the newly optimised tables and data should be created, rather than in place
           --threads        - The number of concurrent connections to use during analysis (default 2)
           --output-file    - The full path to the output file to be generated
           --debug          - Generate Debug Output including SQL Statements being run
           --do-execute     - Run the compression encoding optimisation
           --slot-count     - Modify the wlm_query_slot_count from the default of 1
           --ignore-errors  - Ignore errors raised in threads when running and continue processing
           --force          - Force table migration even if the table already has Column Encoding applied
           --drop-old-data  - Drop the old version of the data table, rather than renaming
           --comprows       - Set the number of rows to use for Compression Encoding Analysis
```

## Specific Usage Notes

There are a few runtime options that deserve a bit more explanation:

### Threads

The Column Encoding utility runs multiple threads to speed the generation of the output script. This means that if requested, it will consume all of the requested capacity for the Queue in which it runs. This may impact other users who are trying to run queries in this queue, and so care should be taken when running with a high number of threads. Please see http://docs.aws.amazon.com/redshift/latest/dg/cm-c-defining-query-queues.html for more information about WLM Query Queue Configuration.

### Slot Count

Within a given queue, each session will be given a single concurrency slot. In some cases, particularly when the ```--do-execute true``` option is used, you may want to use a low thread count and an increased slot count ```--slot-count N```. This will increase the amount of memory that a given session has access to, up to the limit imposed by the Query Queue. This will result in the INSERT...SELECT... used to migrate the data to the new table structure potentially running faster. However, as stated on the Threads option, you must consider the impact on other users. It is advised that Threads * SlotCount is less than the Queue Concurrency.

### Comprows

By default, the ANALYZE COMPRESSION command will attempt to analyze 100,000 rows across all Slices on the Cluster. For some types of data, you may wish to increase this value to get better coverage across all rows stored in the table.

### Do Execute

This option will cause the encoding utility to run the generated script as it goes. Changes will be made to your database LIVE and cannot be undone. It is not recommended that you use this option on Production systems. Furthermore, if the ```--drop-old-data true``` option is included with ```--do-execute true```, then you will be required to confirm that you wish to run this operation before the utility will proceed.


----

Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

    http://aws.amazon.com/asl/
