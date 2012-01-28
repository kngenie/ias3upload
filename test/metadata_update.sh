#!/bin/bash
#
function wait_meta_update {
  D=30
  echo "pausing $D seconds..."; sleep $D
}
META_UPDATE_DELAY=5
W=test
./ias3upload.pl -n -v -R -c test_collection -f -l $W/upload_test_item.csv || {
    echo first ias3upload failed.
    exit 1
}
wait_meta_update
C=$$-$(date +%s)
M=$W/metadata-$$.csv
cat >$M <<EOF
item,file,cookie
GoBoardAndStoneBowls,,$C
EOF
trap "rm -f $M" EXIT 
./ias3upload.pl -v --update-metadata -l $M || {
    echo second ias3upload failed.
    exit 1
}
wait_meta_update
curl -s http://www.archive.org/metadata/GoBoardAndStoneBowls | grep -o '"cookie":[^,]*'
