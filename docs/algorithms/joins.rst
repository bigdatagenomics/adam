ShuffleRegionJoin Load Balancing
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

ShuffleRegionJoins perform a sort-merge join on distributed genomic
data. The current standard for distributing genomic data are to use a
binning approach where ranges of genomic data are assigned to a
particular partition. This approach has a significant limitation that we
aim to solve: no matter how fine-grained the bins created, they can
never resolve extremely skewed data. ShuffleRegionJoin also requires
that the data be sorted, so we keep track of the fact that knowledge of
sort through the join so we can reuse this knowledge downstream.

The first step in ShuffleRegionJoin is to sort and balance the data.
This is done with a sampling method and the data are sorted if it was
not previously. When we shuffle the data, we also store the region
ranges for all the data on this partition. Storing these partition
bounds allows us to copartition the right dataset by assigning all
records to a partition if the record falls within the partition bounds.
After the right data are colocated with the correct records in the left
dataset, we perform the join locally on each partition.

Maintaining the sorted knowledge and partition bounds are extremely
useful for downstream applications that can take advantage of sorted
data. Subsequent joins, for example, will be much faster because the
data are already relatively balanced and sorted. Additional set theory
and aggregation primitives, such as counting nearby regions, grouping
and clustering nearby regions, and finding the set difference will all
benefit from the sorted knowledge because each of these primitives
requires that the data be sorted first.

