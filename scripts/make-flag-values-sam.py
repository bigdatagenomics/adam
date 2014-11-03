#!/usr/bin/python

"""
Script for generating the 'flag-values.sam' file used by ViewSuite.

Generates a SAM file with one contig and one read for every possible flag-field value that HTSJDK's SAM-parsing logic
accepts (i.e. it rejects reads that are listed as "unpaired" (0x1) but have bits set that are only relevant to paired
reads (0x2, 0x8, 0x20, 0x40, 0x80)).

The total number of reads that this script outputs is 1000, broken down as follows:

  - There are 4096 (2^12) possible values of the flag-field
    - 2048 have 0x4 (unmapped) set:
        - only 1/4 (512) of these are allowed, because neither 0x100 nor 0x800 can be set if 0x4 is set.
    - 2048 don't have 0x4 set.
  - There are only 2560 (2048 + 512 from the above bullets) candidates left.
    - Half (1280) have 0x1 set:
      - Of these, 1/4 (320) have neither 0x40 (first in template) nor 0x80 (second in template) set and are therefore
        invalid.
      - Only the other 3/4 (960) remain.
    - Half (1280) don't have 0x1 set:
      - Of these, only 1/32nd (40) of them have none of {0x2, 0x8, 0x20, 0x40, 0x80} set and are valid.
  - 960 from the "paired" (0x1) block and 40 from the "unpaired" block make 1000 total.
"""

from copy import copy

fd = open('adam-cli/src/test/resources/flag-values.sam', 'w')

sq_line='@SQ	SN:1	LN:249250621\n'
fd.write(sq_line)

read='read:0	0	1	1	60	75M	%s	%s	0	GTATAAGAGCAGCCTTATTCCTATTTATAATCAGGGTGAAACACCTGTGCCAATGCCAAGACAGGGGTGCCAAGA	*	NM:i:0	AS:i:75	XS:i:0'.split('\t')

for i in range(4096):

    # If read is marked as paired, exactly one of "first in template" or "second in template" must be set.
    if i & 1 and not (i & 0x40) and not (i & 0x80):
        continue

    # Unpaired reads shouldn't have several flags set
    if (not (i & 1) and (i & 2 or i & 8 or i & 0x20 or i & 0x40 or i & 0x80)):
        continue

    # Unmapped reads shouldn't have "reverse strand", "secondary alignment" or "supplementary alignment" flags set.
    if (i & 4 and (i & 0x10 or i & 0x100 or i & 0x800)):
        continue

    if (not i & 0x100 and i & 0x800):
        continue

    x = copy(read)
    x[0] = 'read:' + str(i)
    x[1] = str(i)
    x[2] = '*' if i & 4 else '1'
    x[3] = '0' if i & 4 else '1'
    x[4] = '0' if i & 4 else '60'
    x[5] = '*' if i & 4 else '75M'
    x[6] = '*' if (i & 8 or not i&1) else '1'
    x[7] = '0' if (i & 8 or not i&1) else '1'
    fd.write('\t'.join(x) + '\n')

fd.close()
