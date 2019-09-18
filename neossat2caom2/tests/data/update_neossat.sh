#!/bin/bash

for ii in expected*.xml; do
  echo $ii
  x="${ii/expected\./}"
  y="${x/\.xml/}"
  echo $y
  caom2-repo delete --cert $HOME/.ssl/cadcproxy.pem --resource-id ivo://cadc.nrc.ca/sc2repo NEOSSAT $y
  caom2-repo create --cert $HOME/.ssl/cadcproxy.pem --resource-id ivo://cadc.nrc.ca/sc2repo $ii
done
