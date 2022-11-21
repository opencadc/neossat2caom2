#!/bin/bash

if [[ ! -e ${PWD}/config.yml ]]
then
  cp /usr/local/.config/config.yml ${PWD}
fi

if [[ ! -e ${PWD}/state.yml ]]; then
  if [[ "${@}" == "neossat_run_state" ]]; then
    yesterday=$(date -d yesterday "+%d-%b-%Y %H:%M")
    echo "bookmarks:
    neossat_timestamp:
      last_record: $yesterday
context:
  neossat_context:
    - NESS
    - 2017
    - 2018
    - 2019
    - 2020
    - 2021
" > ${PWD}/state.yml
  else
    cp /usr/local/.config/state.yml ${PWD}
  fi
fi

exec "${@}"
