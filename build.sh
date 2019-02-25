#!/bin/bash

make build-linux
rm -rf build/node*
make localnet-start
