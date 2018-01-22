#!/usr/bin/env python

from __future__ import print_function
import sys
import time

# read lines from stdin
lines = sys.stdin.readlines()

def print_lines(skip_header=False):
    for line in lines:
        if not (skip_header and line.startswith('@')):
            print(line.strip().rstrip())

print_lines()
sys.stdout.flush()

time.sleep(10)

print_lines(skip_header=True)
sys.stdout.flush()
