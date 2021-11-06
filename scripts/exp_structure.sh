#!/bin/bash

time ../release/exp_structure \
  --input_filename=/local/scratch/wellenzohn/datasets/swh/dataset.shuffled.16384.partition \
  --index_file=/local/scratch/wellenzohn/workspace/indexes/swh/index.bin \
  --partition_folder=/local/scratch/wellenzohn/workspace/partitions/ \
  --mem_size=100000000000 \
  > /local/scratch/wellenzohn/code/scalable_rcas/experiments/08_structure/swh.txt

# time ../release/exp_structure \
#   --input_filename=/local/scratch/wellenzohn/datasets/serverfarm/sf_size.part16 \
#   --index_file=/local/scratch/wellenzohn/workspace/indexes/serverfarm/index.bin \
#   --partition_folder=/local/scratch/wellenzohn/workspace/partitions/ \
#   --mem_size=20000000000 \
#   > /local/scratch/wellenzohn/code/scalable_rcas/experiments/08_structure/serverfarm.txt

# time ../release/exp_structure \
#   --input_filename=/local/scratch/wellenzohn/datasets/amazon/amazon.part16 \
#   --index_file=/local/scratch/wellenzohn/workspace/indexes/amazon/index.bin \
#   --partition_folder=/local/scratch/wellenzohn/workspace/partitions/ \
#   --mem_size=20000000000 \
#   > /local/scratch/wellenzohn/code/scalable_rcas/experiments/08_structure/amazon.txt

# time ../release/exp_structure \
#   --input_filename=/local/scratch/wellenzohn/datasets/xmark/xmark.part16 \
#   --index_file=/local/scratch/wellenzohn/workspace/indexes/xmark/index.bin \
#   --partition_folder=/local/scratch/wellenzohn/workspace/partitions/ \
#   --mem_size=20000000000 \
#   > /local/scratch/wellenzohn/code/scalable_rcas/experiments/08_structure/xmark.txt

# time ../release/exp_structure \
#   --input_filename=/local/scratch/wellenzohn/datasets/swh/dataset.shuffled.16384.partition \
#   --index_file=/local/scratch/wellenzohn/workspace/indexes/index.bin \
#   --partition_folder=/local/scratch/wellenzohn/workspace/partitions/ \
#   --input_size=1000000000 \
#   --mem_size=20000000000
