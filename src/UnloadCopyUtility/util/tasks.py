from abc import abstractmethod
import uuid
import sys
import time
import copy
import logging
from global_config import config_parameters
from util.resources import Resource
from util.s3_utils import S3Helper


class TaskManager(object):
    def __init__(self):
        self.tasks = {}
        self.completed_successfully_tasks = {}
        self.completed_failed_tasks = {}

    def add_task(self, task, dependency_of=None, dependencies=None):
        if task.task_id in self.tasks.keys():
            raise TaskManager.DuplicateTaskException()
        self.tasks[task.task_id] = task
        if dependency_of is not None:
            if isinstance(dependency_of, list):
                for downstream_task in dependency_of:
                    self.add_dependency_to_task(task=downstream_task, dependency=task)
            else:
                self.add_dependency_to_task(task=dependency_of, dependency=task)
        if dependencies is not None:
            if isinstance(dependencies, list):
                for dependency in dependencies:
                    self.add_dependency_to_task(task=task, dependency=dependency)
            else:
                self.add_dependency_to_task(task=task, dependency=dependencies)

    def add_dependency_to_task(self, task, dependency):
        self.get_task(task).dependencies.append(DependencyList.get_safe_value(dependency))

    def get_task(self, task_or_task_id):
        return self.tasks[DependencyList.get_safe_value(task_or_task_id)]

    class DuplicateTaskException(Exception):
        def __init__(self):
            super(TaskManager.DuplicateTaskException, self).__init__()

    def run(self):
        while len(self.tasks.keys()) > 0:
            tasks_clone = self.tasks.copy()
            for task_id in tasks_clone.keys():
                self.remove_fulfilled_dependencies(task_id)
                if len(self.tasks[task_id].dependencies) == 0:
                    task = self.tasks.pop(task_id)
                    # noinspection PyBroadException
                    try:
                        task.execute()
                        self.mark_task_as_succeeded(task)
                    except Exception as e:
                        logging.warning(e)
                        self.mark_task_as_failed(task)
                        if config_parameters['failOnError']:
                            logging.fatal('Task {t} fails and failOnError is True.'.format(t=task))
                            sys.exit(2)
                else:
                    logging.debug('Task {t} has {n} unmet dependencies.'.format(
                        t=self.tasks[task_id],
                        n=len(self.tasks[task_id].dependencies)
                    ))
                    for dependency in self.tasks[task_id].dependencies:
                        logging.debug('\t{d}'.format(d=dependency))
            time.sleep(1)

    def remove_fulfilled_dependencies(self, task_id):
        for dependency in self.tasks[task_id].dependencies.copy():
            if dependency in self.completed_successfully_tasks.keys():
                logging.debug('Dependency {d} for task {t} succeeded earlier, clearing dependency'.format(
                    d=dependency,
                    t=task_id
                ))
                self.tasks[task_id].dependencies.remove(dependency)
            elif dependency in self.completed_failed_tasks.keys():
                logging.debug('Dependency {d} for task {t} failed earlier, failing {t} task as well.'.format(
                    d=dependency,
                    t=task_id
                ))
                self.tasks[task_id].has_failed = True
                self.tasks[task_id].dependencies.remove(dependency)

    def mark_task_as_succeeded(self, task):
        logging.info('Task succeeded {t}'.format(t=task))
        self.completed_successfully_tasks[task.task_id] = task
        logging.debug('All succeeded tasks: {tl}'.format(tl=self.completed_successfully_tasks))

    def mark_task_as_failed(self, task):
        logging.info('Task failed {t}'.format(t=task))
        self.completed_failed_tasks[task.task_id] = task
        logging.debug('All failed tasks: {tl}'.format(tl=self.completed_failed_tasks))


class DependencyList(list):
    def append(self, value):
        return super(DependencyList, self).append(DependencyList.get_safe_value(value))

    def count(self, value):
        return super(DependencyList, self).count(DependencyList.get_safe_value(value))

    def index(self, value, start=0, stop=None):
        return super(DependencyList, self).index(DependencyList.get_safe_value(value), start, stop)

    def remove(self, value):
        return super(DependencyList, self).remove(DependencyList.get_safe_value(value))

    def copy(self):
        return copy.deepcopy(self)

    def __setitem__(self, key, value):
        return super(DependencyList, self).__setitem__(key, DependencyList.get_safe_value(value))

    @staticmethod
    def get_safe_value(value):
        if isinstance(value, Task):
            value = value.task_id
        if isinstance(value, uuid.UUID):
            return value
        raise ValueError('Value {v} cannot be converted to valid dependency (task_id)'.format(v=value))


class Task(object):
    def __init__(self, source_resource=None, target_resource=None, s3_details=None):
        self.source_resource = source_resource
        self.target_resource = target_resource
        self.s3_details = s3_details
        self.dependencies = DependencyList()
        self.task_id = uuid.uuid4()
        self.has_failed = False

    @abstractmethod
    def execute(self):
        """
        Should peform the task and raise exceptions if anything goes wrong.
        :return:
        """
        pass

    def __str__(self):
        return self.__class__.__name__ + '(' + str(self.task_id) + ')'


class FailIfResourceDoesNotExistsTask(Task):
    def __init__(self, resource=None):
        super(FailIfResourceDoesNotExistsTask, self).__init__(source_resource=resource,
                                                              target_resource=None,
                                                              s3_details=None)

    def execute(self):
        if not self.source_resource.is_present():
            raise Resource.NotFound('{r} was not found'.format(r=self))


class FailIfResourceClusterDoesNotExistsTask(Task):
    def __init__(self, resource=None):
        super(FailIfResourceClusterDoesNotExistsTask, self).__init__(source_resource=resource,
                                                                     target_resource=None,
                                                                     s3_details=None)

    def execute(self):
        res = self.source_resource.get_cluster().get_query_full_result_as_list_of_dict('select 1 as result')
        if not res[0]['result'] == 1:
            raise Resource.NotFound('Cluster of resource {r} could not be queried.'.format(r=self.source_resource))


class NoOperationTask(Task):
    def __init__(self):
        super(NoOperationTask, self).__init__()

    def execute(self):
        return


class CreateIfTargetDoesNotExistTask(Task):
    def __init__(self, source_resource=None, target_resource=None):
        super(CreateIfTargetDoesNotExistTask, self).__init__(source_resource=source_resource,
                                                             target_resource=target_resource,
                                                             s3_details=None)

    def execute(self):
        if config_parameters['destinationTableForceDropCreate']:
            logging.info('Dropping target table {tbl}'.format(tbl=str(self.target_resource)))
            self.target_resource.drop()

        if not self.target_resource.is_present():
            self.target_resource.clone_structure_from(self.source_resource)
            logging.info('Creating target {tbl}'.format(tbl=str(self.target_resource)))
            self.target_resource.create()


class UnloadDataToS3Task(Task):
    def __init__(self, cluster_resource, s3_details):
        super(UnloadDataToS3Task, self).__init__(source_resource=cluster_resource,
                                                 target_resource=None,
                                                 s3_details=s3_details)

    def execute(self):
        logging.info("Exporting from Source ({t})".format(t=self))
        self.source_resource.unload_data(self.s3_details)


class CopyDataFromS3Task(Task):
    def __init__(self, cluster_resource, s3_details):
        super(CopyDataFromS3Task, self).__init__(source_resource=None,
                                                 target_resource=cluster_resource,
                                                 s3_details=s3_details)

    def execute(self):
        logging.info("Importing to Target ({t})".format(t=self))
        self.target_resource.copy_data(self.s3_details)


class CleanupS3StagingAreaTask(Task):
    def __init__(self, s3_details):
        super(CleanupS3StagingAreaTask, self).__init__(source_resource=None,
                                                       target_resource=None,
                                                       s3_details=s3_details)

    def execute(self):
        s3_helper = S3Helper(config_parameters['region'])
        if self.s3_details.deleteOnSuccess:
            s3_helper.delete_s3_prefix(self.s3_details)
