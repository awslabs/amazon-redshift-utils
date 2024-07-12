#!/usr/bin/env python
"""
Usage:

python redshift-unload-copy.py <config file> <region>


* Copyright 2014, Amazon.com, Inc. or its affiliates. All Rights Reserved.
* SPDX-License-Identifier: Apache-2.0
"""

import json
import sys
import logging
import util.log
from global_config import GlobalConfigParametersReader, config_parameters
from util.s3_utils import S3Helper, S3Details
from util.redshift_cluster import RedshiftCluster
from util.resources import ResourceFactory, TableResource, DBResource, SchemaResource
from util.tasks import TaskManager, FailIfResourceDoesNotExistsTask, CreateIfTargetDoesNotExistTask, \
    FailIfResourceClusterDoesNotExistsTask, UnloadDataToS3Task, CopyDataFromS3Task, CleanupS3StagingAreaTask, \
    NoOperationTask

logger = util.log.setup_custom_logger('UnloadCopy')
logger.info('Starting the UnloadCopy Utility')

region = None

encryptionKeyID = 'alias/RedshiftUnloadCopyUtility'


def usage():
    print("Redshift Unload/Copy Utility")
    print("Exports data from a source Redshift database to S3 as an encrypted dataset, "
          "and then imports into another Redshift Database")
    print("")
    print("Usage:")
    print("python redshift_unload_copy.py <configuration> <region>")
    print("    <configuration> Local Path or S3 Path to Configuration File on S3")
    print("    <region> Region where Configuration File is stored (S3) "
          "and where Master Keys and Data Exports are stored")
    sys.exit(-1)


class ConfigHelper:
    def __init__(self, config_path, s3_helper=None):
        self.s3_helper = s3_helper

        if config_path.startswith("s3://"):
            if s3_helper is None:
                raise Exception('No region set to get config file but it resides on S3')
            self.config = s3_helper.get_json_config_as_dict(config_path)
        else:
            with open(config_path) as f:
                self.config = json.load(f)


class UnloadCopyTool:
    # noinspection PyDefaultArgument
    def __init__(self,
                 config_file,
                 region_name,
                 global_config_values=GlobalConfigParametersReader().get_default_config_key_values()):
        for key, value in global_config_values.items():
            config_parameters[key] = value
        self.region = region_name
        self.s3_helper = S3Helper(self.region)

        # load the configuration
        self.config_helper = ConfigHelper(config_file, self.s3_helper)

        self.task_manager = TaskManager()
        self.barrier_after_all_cluster_pre_tests = NoOperationTask()
        self.task_manager.add_task(self.barrier_after_all_cluster_pre_tests)
        self.barrier_after_all_resource_pre_tests = NoOperationTask()
        self.task_manager.add_task(self.barrier_after_all_resource_pre_tests)

        src_config = self.config_helper.config['unloadSource']
        dest_config = self.config_helper.config['copyTarget']

        if "tableNames" in src_config or "tableName" in src_config:
            self.setup_table_tasks(src_config, dest_config, global_config_values)
        elif "schemaNames" in src_config or "schemaName" in src_config:
            self.setup_schema_tasks(src_config, dest_config, global_config_values)
        else:
            logger.fatal("Invalid configuration, must configure either table or schema")
            raise ValueError("Invalid configuration")

        self.task_manager.run()

    def setup_table_tasks(self, src_config, dest_config, global_config_values):
        if(src_config.get('tableNames', [])):
            src_tables = src_config['tableNames']
            dest_tables = dest_config['tableNames']
            logger.info("Migrating multiple tables")
            if( not dest_tables or len(src_tables) != len(dest_tables) ):
                logger.fatal("When migrating multiple tables 'tableNames' property must be configured in unloadSource and copyTarget, and be the same length")
                raise NotImplementedError
            for idx in range(0,len(src_tables)):
                logger.info("Migrating table: " + src_tables[idx])
                src_config['tableName'] = src_tables[idx]
                dest_config['tableName'] = dest_tables[idx]
                source = ResourceFactory.get_source_resource_from_config_helper(self.config_helper, self.region)
                destination = ResourceFactory.get_target_resource_from_config_helper(self.config_helper, self.region)
                self.add_src_dest_tasks(source,destination,global_config_values)
        else:
            # Migrating a single table
            source = ResourceFactory.get_source_resource_from_config_helper(self.config_helper, self.region)
            destination = ResourceFactory.get_target_resource_from_config_helper(self.config_helper, self.region)
            self.add_src_dest_tasks(source,destination,global_config_values)

    def setup_schema_tasks(self, src_config, dest_config, global_config_values):
        if src_config.get('schemaNames', []):
            src_schemas = src_config['schemaNames']
            dest_schemas = dest_config['schemaNames']
            logger.info("Migrating multiple schemas")
            if not dest_schemas or len(src_schemas) != len(dest_schemas):
                logger.fatal(
                    "When migrating multiple schemas 'schemaNames' property must be configured in unloadSource and copyTarget, and be the same length"
                )
                raise NotImplementedError
            for idx in range(0, len(src_schemas)):
                logger.info("Migrating schema: " + src_schemas[idx])
                src_config['schemaName'] = src_schemas[idx]
                dest_config['schemaName'] = dest_schemas[idx]
                source: SchemaResource = ResourceFactory.get_source_resource_from_config_helper(
                    self.config_helper
                )
                destination: SchemaResource = ResourceFactory.get_target_resource_from_config_helper(
                    self.config_helper
                )
                self.add_src_dest_tasks(source, destination, global_config_values)
        else:
            logger.info("Migrating a single schema")
            source: SchemaResource = ResourceFactory.get_source_resource_from_config_helper(
                self.config_helper
            )
            destination: SchemaResource = ResourceFactory.get_target_resource_from_config_helper(
                self.config_helper
            )
            self.add_src_dest_tasks(source, destination, global_config_values)

    def add_src_dest_tasks(self,source,destination,global_config_values):
        # TODO: Check whether both resources are of type table if that is not the case then perform other scenario's
        if isinstance(source, TableResource):
            if isinstance(destination, DBResource):
                if not isinstance(destination, TableResource):
                    destination = ResourceFactory.get_table_resource_from_merging_2_resources(destination, source)
                if global_config_values['tableName'] and global_config_values['tableName'] != 'None':
                    destination.set_table(global_config_values['tableName'])
                self.add_table_migration(source, destination, global_config_values)
            else:
                logger.fatal('Destination should be a database resource')
                raise NotImplementedError
            pass
        elif isinstance(source, SchemaResource):
            if not isinstance(destination, SchemaResource):
                logger.fatal("Destination should be a schema resource")
                raise NotImplementedError
            self.add_schema_migration(source, destination, global_config_values)
        else:
            # TODO: add additional scenario's
            # For example if both resources are of type schema then create target schema and migrate all tables
            logger.fatal('Source is not a Table, this type of unload-copy is currently not supported.')
            raise NotImplementedError


    def add_table_migration(self, source, destination, global_config_values):
        if global_config_values['connectionPreTest']:
            if not global_config_values['destinationTablePreTest']:
                destination_cluster_pre_test = FailIfResourceClusterDoesNotExistsTask(resource=destination)
                self.task_manager.add_task(destination_cluster_pre_test,
                                           dependency_of=self.barrier_after_all_cluster_pre_tests)
            if not global_config_values['sourceTablePreTest']:
                source_cluster_pre_test = FailIfResourceClusterDoesNotExistsTask(resource=source)
                self.task_manager.add_task(source_cluster_pre_test,
                                           dependency_of=self.barrier_after_all_cluster_pre_tests)
        if global_config_values['destinationTablePreTest']:
            if global_config_values['destinationTableAutoCreate']:
                destination_cluster_pre_test = FailIfResourceClusterDoesNotExistsTask(resource=destination)
                self.task_manager.add_task(destination_cluster_pre_test,
                                           dependency_of=self.barrier_after_all_cluster_pre_tests)
            else:
                destination_table_pre_test = FailIfResourceDoesNotExistsTask(destination)
                self.task_manager.add_task(destination_table_pre_test,
                                           dependency_of=self.barrier_after_all_resource_pre_tests,
                                           dependencies=self.barrier_after_all_cluster_pre_tests)

        if global_config_values['sourceTablePreTest']:
            source_table_pre_test = FailIfResourceDoesNotExistsTask(source)
            self.task_manager.add_task(source_table_pre_test,
                                       dependency_of=self.barrier_after_all_resource_pre_tests,
                                       dependencies=self.barrier_after_all_cluster_pre_tests)

        if global_config_values['destinationTableAutoCreate']:
            create_target = CreateIfTargetDoesNotExistTask(
                source_resource=source,
                target_resource=destination
            )
            self.task_manager.add_task(create_target,
                                       dependency_of=self.barrier_after_all_resource_pre_tests,
                                       dependencies=self.barrier_after_all_cluster_pre_tests)

        s3_details = S3Details(self.config_helper, source, encryption_key_id=encryptionKeyID)
        unload_data = UnloadDataToS3Task(source, s3_details)
        self.task_manager.add_task(unload_data, dependencies=self.barrier_after_all_resource_pre_tests)

        copy_data = CopyDataFromS3Task(destination, s3_details)
        self.task_manager.add_task(copy_data, dependencies=unload_data)

        s3_cleanup = CleanupS3StagingAreaTask(s3_details)
        self.task_manager.add_task(s3_cleanup, dependencies=copy_data)

    def add_schema_migration(self, source: SchemaResource, destination: SchemaResource, global_config_values):
        tables = source.list_tables()
        for table_name in tables:
            src_table: TableResource = TableResource(
                source.get_cluster(),
                source.get_schema(),
                table_name
            )
            dest_table: TableResource = TableResource(
                destination.get_cluster(),
                destination.get_schema(),
                table_name
            )
            self.add_table_migration(src_table, dest_table, global_config_values)

def main(args):
    global region

    global_config_reader = GlobalConfigParametersReader()
    global_config_values = global_config_reader.get_config_key_values_updated_with_cli_args(args)

    UnloadCopyTool(global_config_values['s3ConfigFile'], global_config_values['region'], global_config_values)

if __name__ == "__main__":
    main(sys.argv)
