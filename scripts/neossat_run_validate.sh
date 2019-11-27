#!/bin/bash

echo "Get a proxy certificate"
cp $HOME/.ssl/cadcproxy.pem ./ || exit $?

COLLECTION="neossat"
CONTAINER="bucket.canfar.net/${COLLECTION}2caom2"
echo "Get the image ${CONTAINER}"
docker pull ${CONTAINER} || exit $?

echo "Run container ${CONTAINER}"
docker run --rm --name ${COLLECTION}_validate -v ${PWD}:/usr/src/app/ ${CONTAINER} ${COLLECTION}_validate || exit $?

date
exit 0
