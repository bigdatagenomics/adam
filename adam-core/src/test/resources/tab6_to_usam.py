#!/usr/bin/env python

from __future__ import print_function
import sys

# read lines from stdin
lines = sys.stdin.readlines()

# print sam header
print("@HD\tVN:1.5\tSO:unsorted")

# loop and print sam lines
for line in lines:
    fields = line.split()
    firstReadName = fields[0]
    firstSequence = fields[1]
    firstQualities = fields[2]
    secondReadName = fields[3]
    secondSequence = fields[4]
    secondQualities = fields[5]

    # flags:
    # 1 = paired (we assume that in this script)
    # 4 = unmapped
    # 8 = mate unmapped
    # 64 = first of pair
    # 128 = second of pair
    firstFlags = 64 | 8 | 4 | 1
    secondFlags = 128 | 8 | 4 | 1

    # sam is the following tab-delimited columns:
    #
    # 1. read name
    # 2. flags
    # 3. ref (* = unaligned)
    # 4. pos (0 = unaligned)
    # 5. map qual (0 if unmapped)
    # 6. cigar (* = unavailable)
    # 7. mate ref (* = unaligned)
    # 8. mate pos (0 = unaligned)
    # 9. tlen (0 = unknown)
    # 10. sequence
    # 11. qualities
    print("%s\t%d\t*\t0\t0\t*\t*\t0\t0\t%s\t%s" % (firstReadName,
                                                   firstFlags,
                                                   firstSequence,
                                                   firstQualities))
    print("%s\t%d\t*\t0\t0\t*\t*\t0\t0\t%s\t%s" % (secondReadName,
                                                   secondFlags,
                                                   secondSequence,
                                                   secondQualities))
