Core Algorithms
===============

Read Preprocessing Algorithms
-----------------------------

In ADAM, we have implemented the three most-commonly used pre-processing
stages from the GATK pipeline (DePristo et al. 2011). In this section,
we describe the stages that we have implemented, and the techniques we
have used to improve performance and accuracy when running on a
distributed system. These pre-processing stages include:

-  *Duplicate Removal:* During the process of preparing DNA for
   sequencing, reads are duplicated by errors during the sample
   preparation and polymerase chain reaction stages. Detection of
   duplicate reads requires matching all reads by their position and
   orientation after read alignment. Reads with identical position and
   orientation are assumed to be duplicates. When a group of duplicate
   reads is found, each read is scored, and all but the highest quality
   read are marked as duplicates. We have validated our duplicate
   removal code against Picard (The Broad Institute of Harvard and MIT
   2014), which is used by the GATK for Marking Duplicates. Our
   implementation is fully concordant with the Picard/GATK duplicate
   removal engine, except we are able to perform duplicate marking for
   chimeric read pairs. [2]_ Specifically, because Picard's traversal
   engine is restricted to processing linearly sorted alignments, Picard
   mishandles these alignments. Since our engine is not constrained by
   the underlying layout of data on disk, we are able to properly handle
   chimeric read pairs.
-  *Local Realignment:* In local realignment, we correct areas where
   variant alleles cause reads to be locally misaligned from the
   reference genome. [3]_ In this algorithm, we first identify regions
   as targets for realignment. In the GATK, this identification is done
   by traversing sorted read alignments. In our implementation, we fold
   over partitions where we generate targets, and then we merge the tree
   of targets. This process allows us to eliminate the data shuffle
   needed to achieve the sorted ordering. As part of this fold, we must
   compute the convex hull of overlapping regions in parallel. We
   discuss this in more detail later in this section. After we have
   generated the targets, we associate reads to the overlapping target,
   if one exists. After associating reads to realignment targets, we run
   a heuristic realignment algorithm that works by minimizing the
   quality-score weighted number of bases that mismatch against the
   reference.
-  *Base Quality Score Recalibration (BQSR):* During the sequencing
   process, systemic errors occur that lead to the incorrect assignment
   of base quality scores. In this step, we label each base that we have
   sequenced with an error covariate. For each covariate, we count the
   total number of bases that we saw, as well as the total number of
   bases within the covariate that do not match the reference genome.
   From this data, we apply a correction by estimating the error
   probability for each set of covariates under a beta-binomial model
   with uniform prior. We have validated the concordance of our BQSR
   implementation against the GATK. Across both tools, only 5000 of the
   180B bases (:math:`<0.0001\%`) in the high-coverage NA12878 genome
   dataset differ. After investigating this discrepancy, we have
   determined that this is due to an error in the GATK, where paired-end
   reads are mishandled if the two reads in the pair overlap.
-  *ShuffleRegionJoin Load Balancing:* Because of the non-uniform
   distribution of regions in mapped reads, joining two genomic datasets
   can be difficult or impossible when neither dataset fits completely
   on a single node. To reduce the impact of data skew on the runtime of
   joins, we implemented a load balancing engine in ADAM's
   ShuffleRegionJoin core. This load balancing is a preprocessing step
   to the ShuffleRegionJoin and improves performance by 10–100x. The
   first phase of the load balancer is to sort and repartition the left
   dataset evenly across all partitions, regardless of the mapped
   region. This offers significantly better distribution of the data
   than the standard binning approach. After rebalancing the data, we
   copartition the right dataset with the left based on the region
   bounds of each partition. Once the data has been copartitioned, it is
   sorted locally and the join is performed.

In the rest of this section, we discuss the high level implementations
of these algorithms.

.. [2]
   In a chimeric read pair, the two reads in the read pairs align to
   different chromosomes; see Li et al (Li and Durbin 2010).

.. [3]
   This is typically caused by the presence of insertion/deletion
   (INDEL) variants; see DePristo et al (DePristo et al. 2011).

