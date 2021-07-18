# The Scalable RCAS+ Index

This is the code for the following paper:

- **Scalable Content-and-Structure Indexing**
- Kevin Wellenzohn, Michael Böhlen, Sven Helmer, Antoine Pietri, Stefano Zacchiroli

## Reproducibility

To reproduce the findings in this paper please follow the instructions in the
[reproducibility package](REPRODUCIBILITY.md).


## Compilation

The code is written in C++17. You need
- A C++17 compliant compiler
- CMake


### Compiling in RELEASE Mode:

Compiling in RELEASE mode turns on optimizations:

```
mkdir release
cd release
cmake .. -DCMAKE_BUILD_TYPE=Release
make
```


### Compiling in DEBUG Mode:

```
mkdir build
cd build
cmake .. -DCMAKE_BUILD_TYPE=Debug
make
```



### Scratch

Export the first 100GB of a partition to CSV and cut off the last 20 bytes of the revision

```
head -c 99999989760 /local/scratch/wellenzohn/datasets/dataset.16384.partition | ./partition2csv 16384 | ruby ../scripts/shorten_revision.rb > /local/scratch/wellenzohn/datasets/dataset.100GB.csv
```
