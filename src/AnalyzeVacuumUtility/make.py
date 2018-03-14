""" OS-independent Python file for setup """

import os
import subprocess
import sys


def execute_commands(commands, ignores=None):
    """ Executes a list of bash commands for building an environment
    Parameters
    ----------
    commands : list
        strings representing bash commands
    ignores : list
        indexes for commands in the list of commands that should be ignored
        if the command fails
    """

    for i in range(0, len(commands)): # For all commands to run...

        print("Running command '{0}'\n".format(commands[i]))

        try:
            # Run command
            output = subprocess.check_output(commands[i], shell=True)
            print(output.decode(sys.stdout.encoding))

        except subprocess.CalledProcessError:

            # If an error occurred and should not be ignored...
            if ignores is not None and i not in ignores:

                exit(1)


def run():
    """ Run build commands """

    print("Starting build process...")

    root = "/".join(os.path.realpath(__file__).split("/")[0:-1])
    package = {
        "name": "AnalyzeVacuumUtility",
        "python_version": 3.6,
        "envs": [
            {
                "s3_path": "s3://gw-deploy/secure/data-warehouse/aws.env",
                "local_path": "/".join([root, "env", "aws.env", ])
            }
        ]

    }

    commands = [
        "conda remove --name {0} --all --yes".format(package["name"]),
        "conda create --name {0} python=3.6 --yes".format(package["name"]),
        " && ".join([
            "source activate {0}".format(package["name"]), # Activate new env
            "pip install -r requirements.txt", # Install packages
            "source deactivate", # Deactivate the environment
        ])
    ]

    execute_commands(commands, [0])

    print("\nSetup complete.\n") # Print completion status


run() # Execute environment setup