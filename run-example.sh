#!/usr/bin/env bash
PROJECT_ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
PYTHONPATH="${PROJECT_ROOT}" LUIGI_CONFIG_PATH="${PROJECT_ROOT}/config/luigi.cfg" pipenv run \
luigi --module task PiTask --num-partitions  --local-scheduler