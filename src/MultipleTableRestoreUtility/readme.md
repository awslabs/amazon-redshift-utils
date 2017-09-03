# Multiple Table restore utility
This utility enables the end user to automate the restore of multiple tables using a list of tables in json format. In order to restore several tables you would need to create a list of tables to restore as a json list. See the example json below:
```
{
    "TableRestoreList": [
        {
            "Schemaname": "schema1",
            "Tablename": "table1"
        },
        {
            "Schemaname": "schema2",
            "Tablename": "table2"
        },
        {
            "Schemaname": "schema3",
            "Tablename": "table3"
        }
    ]
}
```
The keys 'TableRestoreList' , 'Schemaname' and 'Tablename' must be used in order for the utility to work. The utility is compatible with python 2.7. 

## Usage
python multitablerestore.py --target-database-name <target database> --source-database-name <source database> --snapshot-identifier <snapshot name> --cluster-identifier <cluster name> --listfile <filename>

The source and target databases can be the same. 

## Limitations
The table cannot be restored to a different schema.
The table can only be restored to the same cluster the snapshot was taken from.
