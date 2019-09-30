# TFCommit

## Installation:
Clone the repository and submodules:
```sh
git clone https://github.com/dannycho7/TFCommit
git submodule update --init --recursive
```
Build the Merkle Hash Tree CPP implementation:
```sh
make -C src/mht-cpp main mhtc.so
```
If you don't have `fastecdsa` installed:
```sh
pip3 install fastecdsa
```
