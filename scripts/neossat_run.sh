#!/bin/bash

echo "Get a proxy certificate"
cp $HOME/.ssl/cadcproxy.pem ./ || exit $?

echo "Get the container"
# docker pull bucket.canfar.net/neossat2caom2 || exit $?
COLLECTION="neossat"
CONTAINER="${COLLECITON}_run_int"

echo "Run container ${CONTAINER}"
docker run --rm --name ${COLLECTION}_run -v ${PWD}:/usr/src/app/ ${CONTAINER} ${COLLECTION}_run || exit $?

date
exit 0
