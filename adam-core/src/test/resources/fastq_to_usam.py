#!/usr/bin/env python

from __future__ import print_function
import sys

# read lines from stdin
lines = sys.stdin.readlines()

# must have multiple of 8
assert len(lines) % 8 == 0, "Expected multiple of 8 lines (got %d -> %s)" % (len(lines), lines)
fastq_records = len(lines) // 4

# print sam header
print("@HD\tVN:1.5\tSO:unsorted")

# loop and print sam lines
for i in range(fastq_records):

    # fastq is:
    #
    # @readname
    # sequence
    # +<optional readname>
    # qualities
    rn1 = lines[4 * i].strip()
    assert rn1[0] == '@'
    readName = rn1[1:-2]
    readNum = rn1[-1:]
    sequence = lines[4 * i + 1].strip()
    rn2 = lines[4 * i + 2]
    assert rn2[0] == '+'
    assert len(rn2.strip()) == 1 or rn2[1:] == readName
    qualities = lines[4 * i + 3].strip()

    # flags:
    # 1 = paired (we assume that in this script)
    # 4 = unmapped
    # 8 = mate unmapped
    # 64 = first of pair
    # 128 = second of pair
    flags = 8 | 4 | 1
    if readNum == '1':
        flags |= 64
    elif readNum == '2':
        flags |= 128
    else:
        assert 1 <= readNum <= 2, "Read num must be 1 or 2: %s from %s" % (readNum, rn1)

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
    print("%s\t%d\t*\t0\t0\t*\t*\t0\t0\t%s\t%s" % (readName,
                                                     flags,
                                                     sequence,
                                                     qualities))
