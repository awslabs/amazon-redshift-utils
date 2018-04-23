# On Demand Redshift

This script will help you to run your Redshift cluster in specific hours of the day based on your requirement. As part of the script, at the end of your scheduled window, this script will take a backup of the cluster before deleting the cluster and restore it from the latest backup based on your requirement. This is automated using data pipeline service template. This is an one time schedule operation.

## Running the Script

Python 3 is required to run the script.

1] Install the boto3 python library:

```bash
sudo pip install boto3
```

2] Download the template “template.json” and the python script “ondemand.py”” that creates a DataPipeliene. 

3] Run the script ondemand.py. This is a one time activity, as the pipeline will be created and will run on schedule based on the  input entered.

```bash
python3 ondemand.py
```

