#!/usr/bin/python

import sys
import os

def getOne(f):
    try:
        firstLine = f.next()
    except StopIteration:
        # No beginning of record found
        return None

    # Read the rest of the record
    retval = [firstLine]

    for i in range(3):
        try:
            nextLine = f.next()
        except StopIteration:
            raise Exception("File ended in the middle of a fastq record")

        retval.append(nextLine)

    return retval

def main(argv):
    if len(argv) != 3:
        sys.stderr.write("Usage: fastq-interleaver <fastq_1> <fastq_2>\n")
        sys.exit(1)

    fastq1 = open(argv[1])
    fastq2 = open(argv[2])

    file1First = False
    orderDetermined = False

    while True:
        next1 = getOne(fastq1)
        next2 = getOne(fastq2)

        # If both files ended, we're done
        if next1==None and next2==None:
            return

        # If one file ended and not the other, throw an error
        if next1==None and next2!=None:
            raise Exception("file 1 ended prematurely")
        if next1!=None and next2==None:
            raise Exception("file 2 ended prematurely")

        # Make sure the read names match
        read1Name = next1[0].strip()
        read2Name = next2[0].strip()

        if read1Name[:-2] != read2Name[:-2]:
            raise Exception("read names don't match: %s != %s" % (read1Name, read2Name))

        if not orderDetermined:
            read1Suffix = read1Name[-2:]
            read2Suffix = read2Name[-2:]

            if read1Suffix == '/1' and read2Suffix == '/2':
                file1First = True
                orderDetermined = True
            elif read1Suffix == '/2' and read2Suffix == '/1':
                file1First = False
                orderDetermined = True
            else:
                raise Exception("Read names '%s' and '%s' don't end in /1 and /2" %
                                (read1Name, read2Name))

        if file1First:
            sys.stdout.write("".join(next1))
            sys.stdout.write("".join(next2))
        else:
            sys.stdout.write("".join(next2))
            sys.stdout.write("".join(next1))
            
main(sys.argv)
