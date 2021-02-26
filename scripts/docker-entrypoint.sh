#!/bin/bash

if [[ ! -e ${PWD}/config.yml ]]
then
  cp /usr/local/bin/config.yml ${PWD}
fi

if [[ ! -e ${PWD}/state.yml ]]; then
  if [[ "${@}" == "neossat_run_state" ]]; then
    yesterday=$(date -d yesterday "+%d-%b-%Y %H:%M")
    echo "bookmarks:
    neossat_timestamp:
      last_record: $yesterday
" > ${PWD}/state.yml
  else
    cp /usr/local/bin/state.yml ${PWD}
  fi
fi

exec "${@}"
