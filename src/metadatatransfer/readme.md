# Metadata transfer utility
This utility enables the end user to automate the transfer of metadata from one cluster to another. Metadata includes users, user profiles, groups, databases, schemas and related privileges. The utility is compatible with python 2.7. 

## Usage
```python metadatatransfer.py --tgtcluster <target cluster endpoint> --srccluster <source cluster endpoint> --tgtuser <target superuser> --srcuser <source superuser> --tgtdbname <target cluster dbname> --srcdbname <source cluster dbname>``` 

## Limitations
While copying object privileges, all or none of the privileges will be transferred to the target. If any privilege fail to apply at target, all privileges will be rolled back for that transaction.