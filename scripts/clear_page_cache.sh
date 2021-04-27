#!/bin/bash

# thanks to https://unix.stackexchange.com/a/87909
free
sync
echo 3 | sudo tee /proc/sys/vm/drop_caches
free
