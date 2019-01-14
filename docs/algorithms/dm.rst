Duplicate Marking Implementation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Reads may be duplicated during sequencing, either due to clonal
duplication via PCR before sequencing, or due to optical duplication
while on the sequencer. To identify duplicated reads, we apply a
heuristic algorithm that looks at read fragments that have a consistent
mapping signature. First, we bucket together reads that are from the
same sequenced fragment by grouping reads together on the basis of read
name and read group. Per read bucket, we then identify the 5' mapping
positions of the primarily aligned reads. We mark as duplicates all read
pairs that have the same pair alignment locations, and all unpaired
reads that map to the same sites. Only the highest scoring read/read
pair is kept, where the score is the sum of all quality scores in the
read that are greater than 15.
