#!/bin/bash

echo "Get a proxy certificate"
cp $HOME/.ssl/cadcproxy.pem ./ || exit $?

COLLECTION="neossat"
CONTAINER="bucket.canfar.net/${COLLECTION}"
echo "Get the image ${CONTAINER}"
docker pull ${CONTAINER} || exit $?

echo "Run container ${CONTAINER}"
docker run --rm --name ${COLLECTION}_run -v ${PWD}:/usr/src/app/ ${CONTAINER} ${COLLECTION}_run || exit $?

date
exit 0
