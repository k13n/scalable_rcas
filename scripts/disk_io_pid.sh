#!/bin/bash

[ "$1" == "" ] && echo "Error: Missing PID" && exit 1
IO=/proc/$1/io          # io data of PID
[ ! -e "$IO" ] && echo "Error: PID does not exist" && exit 2
I=60                     # interval in seconds
SECONDS=0               # reset timer

# cleanup after Ctrl+C
trap "rm $TEMPFILE; exit 1" SIGINT

echo "Watching command $(cat /proc/$1/comm) with PID $1"

IFS=" " read rchar wchar syscr syscw rbytes wbytes cwbytes < <(cut -d " " -f2 $IO | tr "\n" " ")

# create temp file for results
TEMPFILE=$(tempfile)

while [ -e $IO ]; do
    # print current timestamp
    date -u -Ins
    cat $IO > "$TEMPFILE"   # "copy" data
    sed 's/.*/& Bytes/' "$TEMPFILE" | column -t
    echo
    sleep 60
    # sleep 5
done


rm "$TEMPFILE"          # remove temp file
