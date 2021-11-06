#!/bin/bash

# thanks to:
# https://stackoverflow.com/a/24716260

# create temp file for results
TEMPFILE=$(tempfile)

# cleanup after Ctrl+C
trap "rm $TEMPFILE; exit 1" SIGINT

# reset timer
SECONDS=0

# execute command in background
$@ &
# > /local/scratch/wellenzohn/code/scalable_rcas/experiments/14_rcas_insertion/exp_inerstion.01.txt &

# io data of command
IO=/proc/$!/io
while [ -e $IO ]; do
    # print current timestamp
    date -u -Ins
    cat $IO > "$TEMPFILE"   # "copy" data
    sed 's/.*/& Bytes/' "$TEMPFILE" | column -t
    echo
    sleep 60
done

# save timer
S=$SECONDS

echo -e "\nPerformace after $S seconds:"
while IFS=" " read string value; do
    echo $string $(($value/1024/1024/$S)) MByte/s
done < "$TEMPFILE" | column -t

rm "$TEMPFILE"          # remove temp file
