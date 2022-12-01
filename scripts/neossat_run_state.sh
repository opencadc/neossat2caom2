#!/bin/bash

echo "Get a proxy certificate"
cp $HOME/.ssl/cadcproxy.pem ./ || exit $?

COLLECTION="neossat"
CONTAINER="opencadc/${COLLECTION}2caom2"
echo "Get the image ${CONTAINER}"
docker pull ${CONTAINER} || exit $?

echo "Run container ${CONTAINER}"
docker run --rm --name ${COLLECTION}_run -v ${PWD}:/usr/src/app/ --user $(id -u):$(id -g) -e HOME=/usr/src/app ${CONTAINER} ${COLLECTION}_run_state || exit $?

date
exit 0
