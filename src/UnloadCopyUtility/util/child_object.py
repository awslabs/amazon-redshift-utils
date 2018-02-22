

class ChildObject:
    def __init__(self, dependencies=None):
        """

        :param dependencies: A list of Resource objects
        """
        self.dependencies = dependencies or list()

    def are_dependencies_present(self):
        all_present = True
        for dependency in self.dependencies:
            if not dependency.is_present():
                all_present = False
        return all_present

    def create_dependencies(self):
        for dependency in self.dependencies:
            if not dependency.is_present():
                dependency.create()
