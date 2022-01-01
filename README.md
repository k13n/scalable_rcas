# Robust and Scalable Content-and-Structure Indexing

This is the code for the following paper:

- **Robust and Scalable Content-and-Structure Indexing**
- Kevin Wellenzohn, Michael Böhlen, Sven Helmer, Antoine Pietri, Stefano Zacchiroli

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


## Datasets

The four datasets included in the paper can be downloaded here:

- [GitLab](https://download.ifi.uzh.ch/dbtg/wellenzohn/scalable_rcas/gitlab.part16)
- [ServerFarm](https://download.ifi.uzh.ch/dbtg/wellenzohn/scalable_rcas/sf_size.part16)
- [Amazon](https://download.ifi.uzh.ch/dbtg/wellenzohn/scalable_rcas/amazon.part16)
- [XMark](https://download.ifi.uzh.ch/dbtg/wellenzohn/scalable_rcas/xmark.part16)
