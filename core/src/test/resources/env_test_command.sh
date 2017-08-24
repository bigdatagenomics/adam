#!/bin/bash

# pipe input to a file
tee ${OUTPUT_PATH} > /dev/null

# print out another file
cat ${INPUT_PATH} | tee ${OUTPUT_PATH}_2