#!/bin/bash

# pipe input to a file
tee $1 > /dev/null

# print out another file
cat $2