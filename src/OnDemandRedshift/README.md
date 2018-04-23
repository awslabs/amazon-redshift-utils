# On Demand Redshift

This script will help you to run your Redshift cluster in specific hours of the day based on your requirement. As part of the script, at the end of your scheduled window, this script will take a backup of the cluster before deleting the cluster and will restore it from the latest backup based on your requirement. It is automated using data pipeline service template and is a one time schedule operation.

## Running the Script

Python 2 is required to run this script.

1] Install the boto3 python library:

```bash
sudo pip install boto3
```

2] Download the template “template.json” and the python script “ondemand.py”” that creates a DataPipeliene.

For example, you can use 'wget' command to download these files from this github repository:

```bash
wget https://raw.githubusercontent.com/suvenduk/amazon-redshift-utils/master/src/OnDemandRedshift/ondemand.py
wget https://raw.githubusercontent.com/suvenduk/amazon-redshift-utils/master/src/OnDemandRedshift/template.json
```

3] Run the script ondemand.py. This is a one time activity, as the pipeline will be created and will run on schedule based on the  input entered.

```bash
python ondemand.py
```

Please follow the instructions in the script to process further.
