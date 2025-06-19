 User Guide: Migrating Redshift Local Users to AWS IAM IDC 


This document guides on how to use the utility to migrate local users, groups and roles from your Redshift instance to AWS IAM Identity Center (IDC) users, groups and roles.

Utility Version: 1.0
Release Date: 05/31/2025

Scope:
Following are the activities performed by this utility.

1. Create users in IDC for every local user in given Redshift instance.
2. Create groups in IDC for every group and/or role in given Redshift instance.
3. Assign users to groups in IDC as per existing assignments in the Redshift instance.
4. Create IDC roles in Redshift instance matching the IDC group names.  
5. Grants permissions to IDC roles in Redshift instance based on the current permissions given to local groups and roles.


Considerations:

* Creating permissions in AWS Lake Formation is currently not in scope
* IDC and IDP integration setup is out of scope for this utility.  However, “vw_local_ugr_to_idc_urgr_priv.sql” can be used to create roles and grant permissions to the IDP users/groups passed through IDC.
    
* If you have any permissions given directly to local user IDs i.e not via groups or roles, then you need to change that to a role based permission approach for IDC integration.  Please create roles and provide permissions via roles instead of directly giving permissions to users.


Utility Artifacts:

Please download the utility artifacts.

* `idc_redshift_unload_indatabase_groups_roles_users.py`:  python script to unload users, groups , roles and their associations.
* `redshift_unload.ini`: Config file used in above script to read redshift cluster details and S3 locations to unload the files.
* `idc_add_users_groups_roles_psets.py`: python script to create users, groups in IDC and then associate the users to groups in IDC.
* `idc_config.ini`: Config file used in the above script to read IDC details.
* `vw_local_ugr_to_idc_urgr_priv.sql`: Generates SQL statements to create IDC roles in Redshift matching with exact names as IDC groups.  It also generates SQLs to GRANT permissions to IDC roles created in Redshift.



Pre-requisites:
Following are the pre-requisites before running the utility.

* Enable AWS IDC in your account.  Please refer Enabling AWS IAM Identity Center documentation.
* Complete steps 1 to 8 from the blog Integrate Identity Provider (IdP) with Amazon Redshift Query Editor V2 and SQL Client using AWS IAM Identity Center for seamless Single Sign-On.
* Disable assignments for IDC application.
    * In IDC console, navigate to Application Assignments → Applications.
    * Select the application.  Choose Actions→ Edit details.
    * Set “User and group assignments” to “Do not require assignments”. After Redshift-idc application is set up in above step, for testing purposes , edit the details of the  application in Identity Center and chose user and group membership not required, so connectivity to Redshift can be tested even without data access
* Enable Identity Center authentication with admin access from ec2 or cloud shell . https://docs.aws.amazon.com/sdkref/latest/guide/access-sso.html




Run Steps:


1. Update Redshift cluster details and S3 locations in redshift_config.ini
2. Update IDC details in idc_config.ini.  
3. Create a directory in AWS cloudshellor your own EC2 instance which has connectivity to Redshift.
4. Copy the two ini files and download python scripts to that directory
5. Run idc_redshift_unload_indatabase_groups_roles_users.py either from AWS cloudshell or EC2 instance.


python idc_redshift_unload_indatabase_groups_roles_users.py


1. Run idc_add_users_groups_roles_psets.py from AWS cloudshell or EC2 instance.


python idc_add_users_groups_roles_psets.py


1. Connect your Redshift cluster using Query Editor V2 or SQL client of your choice.  Please use superuser credentials.
2. Copy the SQL in “vw_local_ugr_to_idc_urgr_priv.sql” file and run in the editor to create the vw_local_ugr_to_idc_urgr_priv
3. Run following SQL to generate the SQL statement for creating roles and permissions.

select existing_grants,idc_based_grants from vw_local_ugr_to_idc_urgr_priv;

1. Review the statements in idc_based_grants column.  

NOTE: It generates grant statements on 1/ databases 2/schemas 3/tables 4/views 5/columns 6/functions 7/models 8/data shares. However, please note that this may not be 100% comprehensive list of permissions.  So a review is important and the missing ones needs to manually granted.

1. If all is good, run all the statements from the SQL client.


If there is any further help required please reach following contacts
sksonti@amazon.com, shmanee@amazon.com, sumanpun@amazon.com ziadwali@amazon.fr

NOTE: This utility is continuously enhanced to close gaps and add additional functionality.  Please send your issues, feedback, enhancement requests to above contacts with subject “[issue/feedback/enhancement] AWS IDC migration utility from Redshift”


Authors: sksonti@amazon.com, shmanee@amazon.com, sumanpun@amazon.com ziadwali@amazon.fr
