#!/bin/bash

query2="
SELECT fileName
FROM archive_files
WHERE archiveName = 'NEOSS'
"

cadc-tap query --cert $HOME/.ssl/cadcproxy.pem -s ivo://cadc.nrc.ca/ad "${query2}" | grep -v count | grep -v "\-\-\-\-\-\-\-\-" | grep -v "^$" | grep -v affected | grep -v fileName > t
cat t | grep png | awk -F\_ '{print $1}' > s
cat t | grep fits | awk -F\_ '{print $3}' | awk -F\. '{print $1}' >> s
echo "Going to fix:"
cat s | sort | uniq -c | grep -v "   3" | grep -v "   6" | grep -v "   9" | grep -v "  12"

obs_ids=$(cat s | sort | uniq -c | grep -v "   3" | grep -v "   6" | grep -v "   9" | grep -v "  12" | awk '{print $2}' | xargs)
for obs_id in ${obs_ids}; do
    query3="SELECT A.uri from caom2.Artifact as A WHERE A.uri LIKE '%${obs_id}%.fits'"
    cadc-tap query --cert $HOME/.ssl/cadcproxy.pem -s ivo://cadc.nrc.ca/ams/shared "${query3}" | grep -v count | grep -v "\-\-\-\-\-\-\-\-" | grep -v "^$" | grep -v affected | grep -v fileName > t
    f_name=$(cat t | awk -F\/ '{print $2}')
    echo "${f_name}"
done

exit 0
