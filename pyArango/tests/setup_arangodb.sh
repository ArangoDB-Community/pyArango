#!/bin/bash

docker pull arangodb/arangodb-preview:devel
docker run -d -e ARANGO_ROOT_PASSWORD="root" -p 8529:8529 arangodb/arangodb-preview:devel

sleep 2

n=0
# timeout value for startup
timeout=60 
while [[ (-z `curl -H 'Authorization: Basic cm9vdDp0ZXN0' -s 'http://127.0.0.1:8529/_api/version' `) && (n -lt timeout) ]] ; do
  echo -n "."
  sleep 1s
  n=$[$n+1]
done

if [[ n -eq timeout ]];
then
    echo "Could not start ArangoDB. Timeout reached."
    exit 1
fi

echo "ArangoDB is up"
