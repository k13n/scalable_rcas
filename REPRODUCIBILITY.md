
# Reproducibility Package

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Datasets](#datasets)
3. [Compilation](#compilation)
4. [Experiments](#experiments)

This page details the steps needed to reproduce the findings of the
paper:

- **Scalable Content-and-Structure Indexing**
- Kevin Wellenzohn, Michael Böhlen, Sven Helmer, Antoine Pietri, Stefano Zacchiroli


## Prerequisites

Hardware:
- At least 3TB free space
- 400GB main memory (RCAS+ works also with much less memory, but to
  exactly reproduce some experiments 400GB main memory is needed)

Software:
- Linux (we used Debian 10)
- C++ compiler supporting at least C++17 (we used gcc version 8.3.0)
- Ruby (we used version 2.5.5)
- Java (we used openjdk 11.0.9.1 2020-11-04)
- CMake (we used version 3.13.4)
- Postgres (we used version 13.2)

Time:
- Reproducing all experiments takes a lot of time (expect >=3 weeks)



## Datasets

We provide the GitLab dataset in two formats:
- A binary format used by RCAS+ (550GB)
- CSV used by experiments with GNU sort and Postgres (560GB)

The datasets can be downloaded as follows:

```
wget https://files.ifi.uzh.ch/dbtg/wellenzohn/scalable_rcas/dataset.16384.partition
wget https://files.ifi.uzh.ch/dbtg/wellenzohn/scalable_rcas/dataset.csv
```

The format of these files is explained in the following subsections.
You can skip these and jump ahead to the next [section](#compilation).

### Binary Format

The binary format is a list of 16KB pages. Each page has the following
structure:

- `nr_keys` (2 bytes)
- List of `nr_keys` composite keys

A single key has the following structure:

- `len_path`: Length of path (2 bytes)
- `len_value`: Length of value (2 bytes)
- `revision`: The SWH-ID of the revision (SHA1 hash, 20 bytes)
- `path`: The actual path (`len_path` bytes)
- `value`: The actual value (`len_value` bytes)


### CSV Format

A line in the CSV format has the structure "`$path`";`$value`;`$revision`
where

- `$path` is the path of the key and put in double quotes for Postgres
- `$value` is the UNIX timestamp of the revision
- `$revision` is a 20 bytes hexa-decimal string. Note that writing a 20
  bytes SHA1 hash (the revision) as hexa-decimal string requires 40
  bytes. We truncate the hexa-decimal string at 20 bytes such that the
  CSV format and the binary format are approximately the same length.
  The full SHA1 hash can be extracted from the binary format.



## Compilation

RCAS+ is written in C++. After checking out the source code, it can be
compiled as follows:

```bash
mkdir release
cd release
cmake .. -DCMAKE_BUILD_TYPE=Release
make
cd ..
```



## Experiments

Compiling the code creates the following executables in folder `release`
that implement the experiments:

- `exp_dataset_size`
- `exp_dsc_computation`
- `exp_lazy_interleaving`
- `exp_memory_management`
- `exp_page_size`
- `exp_querying`
- `exp_structure`


### Parameters

Many (not all) of the experiments take the following parameters. You
may want to change them to fit your needs. In practice, the first four
parameters are the most relevant parameters.

- `--input_filename`: specifies the full path to the input dataset in
  binary format (see [Datasets](#datasets))
- `--index_file`: specifies the full path to the output file in which
  the bulk-loader stores the final RCAS+ index.  The default value is
  `index.bin`
- `--partition_folder`: specifies the folder that is used by the
  bulk-loading algorithm to store intermediate data (partitions). The
  default value is `partitions/`
- `--mem_size`: specifies the memory size in bytes that the
  bulk-loader is allowed to use to buffer intermediate partitions.
  The default value is 1000000000 (=1GB).
- `--input_size`: specifies the number of bytes of the input dataset
  that should be considered. This allows one to specify a large input
  file but use only a subset of the data as input. The input_size must
  be a multiple of the page size (by default 16384 bytes). An
  input_size of 0 means that the whole input file is considered. The
  default value is 0.
- `--lazy_interleaving`: value 1 specifies that lazy interleaving is
  used and value 0 means lazy interleaving is turned off (in that
  case, full dynamic interleaving is applied). The default value is 1.
- `--partitioning_dsc`: specifies what method is used to determine the
  discriminative byte during the partitioning. This option can take
  one of two values `{proactive, twopass}`. The default is `proactive`.
- `--memory_placement`: specifies what method is used for memory
  management. This option can take one of two values
  `{frontloading, allornothing}`.  The default value is `frontloading`.
- `--direct_io`: specifies if direct I/O is used during partitioning,
  which bypasses the OS caches and writes directly to disk.  Value 1
  specifies that direct I/O is turned on, while value 0 means direct
  I/O is turned off. The default value is 0.


### Scalability Experiment

This experiments compares RCAS+ to RCAS, Postgres, and GNU sort when
we increase the input size.

#### RCAS+ ####

To reproduce the results of RCAS+ run and change the parameters as necessary:

```bash
./release/exp_dataset_size
```

#### RCAS ####

Download the source code of RCAS

```bash
git clone https://github.com/k13n/rcas.git
git checkout scalability_experiment
```

Compile the source code of RCAS as described in its `README` file.

Finally, the file `scripts/rcas_scalability_experiment.rb` in the RCAS
repository implements the RCAS scalability experiment. It has one
parameter:

- `$IN_FILE`: Input dataset in *binary format* (see [Datasets](#datasets))

```bash
ruby scripts/rcas_scalability_experiment.rb $IN_FILE
```

#### GNU sort ####

The file `scripts/gnusort_scalability_experiment.rb` implements the GNU sort
experiment. It takes four parameters

- `$MEM_SIZE`: Memory size in bytes. We used 300000000000 bytes (=300GB)
- `$IN_FILE`: Input dataset in *CSV format* (see [Datasets](#datasets))
  since GNU sort expects a text file as input
- `$TMP_FOLDER`: Folder for temporary files
- `$OUT_FILE`: Output file where the sorted data is written to

```bash
ruby scripts/gnusort_scalability_experiment.rb $MEM_SIZE $IN_FILE $TMP_FOLDER $OUT_FILE
```

#### Postgres ####

This requires a Postgres installation (we compiled version 13.2 from
the source code).

The file `scripts/btree_scalability_experiment.rb` implements the GNU sort
experiment. It takes the following parameters:

- `$PSQL_CMD`: The psql command along with configuration parameters.
  For example, `psql -p 5400 -d rcas` specifies that we connect to
  database `rcas` on port `5400`
- `$IN_FILE`: Input dataset in *CSV format* (see [Datasets](#datasets))
  since postgres expects a text file as input

We script assumes that the user has root access to the postgres
database and can login without a password.

```bash
ruby scripts/btree_scalability_experiment.rb "$PSQL_CMD" $IN_FILE
```


### Query Performance Experiment

We evaluate the query performance on the full GitLab dataset and a
100GB subset.

#### RCAS+ ####

First, we create the two RCAS+ indexes. Replace `$DATASET` with the path
to the GitLab dataset in binary format and add/change other parameters
as needed (see [parameters](#parameters)).

```bash
./release/create_index --input_filename="$DATASET" --index_file="index_full.bin"
./release/create_index --input_filename="$DATASET" --input_size=100000000000 --index_file="index_100GB.bin"
```

The file `scripts/rcasplus_query_performance.rb` implements the query
experiment. It takes the following parameters:

- `$INDEX_FILE`: This denotes the path to the index file created in the
  previous step
- `$QUERY_FILE`: This files contains the CAS queries that are to be
  executed. The queries used in the experiment can be found in the
  folder `queries/cas_queries.csv`

The script can be called as follows:

```bash
ruby scripts/rcasplus_query_performance.rb $INDEX_FILE $QUERY_FILE
```

Given our created indexes from before and the query file, the
experiments can be re-produced with

```bash
ruby scripts/rcasplus_query_performance.rb "index_100GB.bin" "queries/cas_queries.csv"
ruby scripts/rcasplus_query_performance.rb "index_full.bin"  "queries/cas_queries.csv"
```

The script first clears the OS caches with the script
`scripts/clear_page_cache.sh`, which requires root permissions. Then
it runs the queries once on cold caches and 10 times on warm caches.


#### RCAS ####

We assume here that RCAS is checked out and compiled (see steps from
before). The query experiment can be executed with
`release/bm_query_runtime2`. It takes the following parameters:

- `--query_file`: specifies the path to the CAS query file
- `--input_filename`: specifies the full path to the input dataset in
  binary format (see [Datasets](#datasets))
- `--input_size`: specifies the number of bytes of the input dataset
  that should be considered (see [Parameters](#parameters)). The
  default value is 100000000000 (=100GB).
- `--nr_repetitions`: specifies the number of times that a CAS query is
  repeated. The default value is 10.

Execute the query experiment as follows and replace the parameters as
needed:

```bash
./release/bm_query_runtime2 \
  --input_filename="$BINARY_DATASET" \
  --query_file="$QUERY_FILE"
```

#### Postgres ####

First, we create the two RCAS+ indexes. Replace $DATASET with the path
to the GitLab dataset in binary format and add/change other parameters
as needed (see [parameters](#parameters)).

- `$PSQL_CMD`: The psql command along with configuration parameters.
  For example, `psql -p 5400 -d rcas` specifies that we connect to
  database `rcas` at port `5400`
- `$DATASET`: Input dataset in *CSV format* (see [Datasets](#datasets))
  since postgres a text file as input
- `$DATASET_SIZE`: The size in bytes that should be imported from the
  input dataset. If you specify value 0, the whole input dataset is
  imported
- `$TABLE`: The name of the table into which the data is loaded

```bash
ruby ./scripts/create_btree.rb $PSQL_CMD $DATASET 100000000000 "data_100gb"
ruby ./scripts/create_btree.rb $PSQL_CMD $DATASET 0 "data_full"
```

Once the indexes are created, we can query them with the script
`scripts/btree_query_performance.rb`, which takes the following
parameters:

- `$QUERY_FILE`: specifies the path to the CAS query file
- `$TABLE`: The name of the table into which the data is loaded
- `$INDEX`: Can be one of {vp,pv} and specifies which composite index is used

```bash
ruby scripts/btree_query_performance.rb queries/cas_queries.csv data_100gb vp
ruby scripts/btree_query_performance.rb queries/cas_queries.csv data_100gb pv
ruby scripts/btree_query_performance.rb queries/cas_queries.csv data_full vp
ruby scripts/btree_query_performance.rb queries/cas_queries.csv data_full pv
```



### Node Clustering Experiment

This experiment is covered by `exp_structure` that can be executed as
follows (change the parameters as needed).

```bash
./release/exp_structure
```


### Lazy Interleaving Experiment

This experiment is covered by `exp_lazy_interleaving` that can be
executed as follows (change the parameters as needed).

```bash
./release/exp_lazy_interleaving
```

To compare the CAS query performance of RCAS+ with and without lazy
interleaving we first create the two indexes:

```bash
./release/create_index \
  --input_filename="$DATASET" \
  --input_size=100000000000 \
  --index_file="index_lazy_yes.bin"
  --lazy_interleaving=1

./release/create_index \
  --input_filename="$DATASET" \
  --input_size=100000000000 \
  --index_file="index_lazy_no.bin"
  --lazy_interleaving=0
```

Given our created indexes, the query performance of RCAS+ can be evaluated with

```bash
ruby scripts/rcasplus_query_performance.rb "index_lazy_yes.bin" "queries/cas_queries.csv"
ruby scripts/rcasplus_query_performance.rb "index_lazy_no.bin"  "queries/cas_queries.csv"
```



### Proactive Partitioning Experiment

This experiment is covered by `exp_dsc_computation` that can be
executed as follows (change the parameters as needed).

In the paper we report the runtime once without direct I/O and once
with direct I/O.

Without direct I/O:

```bash
./release/exp_dsc_computation --direct_io=0
```

With direct I/O:

```bash
./release/exp_dsc_computation --direct_io=1
```


### Frontloading Experiment

This experiment is covered by `exp_memory_management` that can be
executed as follows (change the parameters as needed).

In the paper we report the runtime once without direct I/O and once
with direct I/O.

Without direct I/O:

```bash
./release/exp_memory_management --direct_io=0
```

With direct I/O:

```bash
./release/exp_memory_management --direct_io=1
```


### Cost Model Experiment

This experiment compares the actual I/O overhead with the estimated
I/O overhead. To compute the actual I/O overhead we build RCAS+ with
program `exp_cost_model`. We compute the estimated I/O overhead with
`io_estimation.rb`.

The actual I/O overhead is computed as follows (change the parameters
as needed). This experiments varies first the input size and then the
memory size.

```bash
./release/exp_cost_model
```

The estimated I/O overhead is computes as follows:


```bash
ruby scripts/io_estimation.rb
```
