#!/bin/bash

set -e

PYTHON=python3
while [ $# -gt 0 ];  do
    case "$1" in
        --instanceUrl)
            shift
            export ARANGODB_URL=$1
            shift
            ;;
        --instanceEndpoint)
            shift
            shift
            # don't care
            ;;
        --auth)
            shift
            shift
            # don't care
            ;;
        --username)
            shift
            export ARANGODB_ROOT_USERNAME=$1
            shift
            ;;
        --password)
            shift
            export ARANGODB_ROOT_PASSWORD=$1
            shift
            ;;
        --enterprise)
            shift
            # don't care
            ;;
        --no-enterprise)
            shift
            # don't care
            ;;
        --host)
            shift
            shift
            # don't care...
            ;;
        --port)
            shift
            shift
            # don't care...
            ;;
        --deployment-mode)
            shift
            shift
            # don't care...
            ;;
        --testsuite)
            shift
            shift
            # TODO: howto pass testsuite filters?
            ;;
        --filter)
            shift
            shift
            # TODO: howto pass testcase filters?
            ;;
        --python-exe)
            shift
            export PYTHON=$1
            shift
            ;;
        *)
            echo "What? my mother was a saint! $1"
            shift
    esac
done

$PYTHON setup.py build

export PYTHONPATH=$(pwd)/build/lib

exec $PYTHON pyArango/tests/tests.py 2>&1
