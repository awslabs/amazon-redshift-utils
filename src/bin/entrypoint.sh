#!/usr/bin/env bash

PROJECT=${1:-}

if [ "${PROJECT}" == "analyze-vacuum" ]; then bin/run-analyze-vacuum-utility.sh
elif [ "${PROJECT}" == "column-encoding" ]; then bin/run-column-encoding-utility.sh
elif [ "${PROJECT}" == "unload-copy" ]; then bin/run-unload-copy-utility.sh
else echo "Unhandled arg for project to run. Please select from either 'analyze-vacuum', 'column-encoding' or 'unload-copy'"
fi