Indel Realignment Implementation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Although global alignment will frequently succeed at aligning reads to
the proper region of the genome, the local alignment of the read may be
incorrect. Specifically, the error models used by aligners may penalize
local alignments containing INDELs more than a local alignment that
converts the alignment to a series of mismatches. To correct for this,
we perform local realignment of the reads against consensus sequences in
a three step process. In the first step, we identify candidate sites
that have evidence of an insertion or deletion. We then compute the
convex hull of these candidate sites, to determine the windows we need
to realign over. After these regions are identified, we generate
candidate haplotype sequences, and realign reads to minimize the overall
quantity of mismatches in the region.

Realignment Target Identification
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To identify target regions for realignment, we simply map across all the
reads. If a read contains INDEL evidence, we then emit a region
corresponding to the region covered by that read.

Convex-Hull Finding
^^^^^^^^^^^^^^^^^^^

Once we have identified the target realignment regions, we must then
find the maximal convex hulls across the set of regions. For a set
`R` of regions, we define a maximal convex hull as the largest
region `\hat{r}` that satisfies the following properties:

.. math::

   \hat{r} &= \cup_{r_i \in \hat{R}} r_i \\
   \hat{r} \cap r_i &\ne \emptyset, \forall r_i \in \hat{R} \\
   \hat{R} &\subset R

In our problem, we seek to find all of the maximal convex hulls, given a
set of regions. For genomics, the convexity constraint is trivial to
check: specifically, the genome is assembled out of reference contigs
that define disparate 1-D coordinate spaces. If two regions exist on
different contigs, they are known not to overlap. If two regions are on
a single contig, we simply check to see if they overlap on that contig's
1-D coordinate plane.

Given this realization, we can define the convex hull Algorithm, which is a data parallel
algorithm for finding the maximal convex hulls that describe a genomic
dataset.

.. math::

    data &\leftarrow \text{input dataset} \\
    regions &\leftarrow data\text{.map}(data \Rightarrow \text{generateTarget}(data)) \\
    regions &\leftarrow regions\text{.sort}() \\
    hulls &\leftarrow regions\text{.fold}(r_1, r_2 \Rightarrow \text{mergeTargetSets}(r_1, r_2))

The ``generateTarget`` function projects each datapoint into a Red-Black
tree that contains a single region. The performance of the fold depends
on the efficiency of the merge function. We achieve efficient merges
with the tail-call recursive ``mergeTargetSets`` function that is
described in the hull set merging algorithm.

.. math::

   first &\leftarrow \text{first target set to merge} \\
   second &\leftarrow \text{second target set to merge} \\
   \text{if} &first = \emptyset \wedge second = \emptyset \\
   &\text{return} \emptyset \\
   \text{else if} &first = \emptyset \\
   &\text{return} second \\
   \text{else if} &second = \emptyset \\
   &\text{return} first \\
   \text{return}& \\
   \text{if} &\text{last}(first) \cap \text{head}(second) = \emptyset \\
   &\text{return} first + second \\
   \text{else}& \\
   &mergeItem \leftarrow (\text{last}(first) \cup \text{head}(second)) \\
   &mergeSet \leftarrow \text{allButLast}(first) \cup mergeItem \\
   &trimSecond \leftarrow \text{allButFirst}(second) \\
   &\text{return mergeTargetSets}(mergeSet, trimSecond)

The set returned by this function is used as an index for mapping reads
directly to realignment targets.

Candidate Generation and Realignment
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Once we have generated the target set, we map across all the reads and
check to see if the read overlaps a realignment target. We then group
together all reads that map to a given realignment target; reads that do
not map to a target are randomly assigned to a "null" target. We do
not attempt realignment for reads mapped to null targets.

To process non-null targets, we must first generate candidate haplotypes
to realign against. We support several processes for generating these
consensus sequences:

-  *Use known INDELs:* Here, we use known variants that were provided by
   the user to generate consensus sequences. These are typically derived
   from a source of common variants such as dbSNP (Sherry et al. 2001).
-  *Generate consensuses from reads:* In this process, we take all
   INDELs that are contained in the alignment of a read in this target
   region.
-  *Generate consensuses using Smith-Waterman:* With this method, we
   take all reads that were aligned in the region and perform an exact
   Smith-Waterman alignment (Smith and Waterman 1981) against the
   reference in this site. We then take the INDELs that were observed in
   these realignments as possible consensuses.

From these consensuses, we generate new haplotypes by inserting the
INDEL consensus into the reference sequence of the region. Per
haplotype, we then take each read and compute the quality score weighted
Hamming edit distance of the read placed at each site in the consensus
sequence. We then take the minimum quality score weighted edit versus
the consensus sequence and the reference genome. We aggregate these
scores together for all reads against this consensus sequence. Given a
consensus sequence `c`, a reference sequence `R`, and a set
of reads :math:`\mathbf{r}`, we calculate this score using the equation
below.

.. math::

   q_{i, j} &= \sum_{k = 0}^{l_{r_i}} Q_k I[r_I(k) = c(j + k)] \forall r_i \in \mathbf{R}, j \in \{0, \dots, l_c - l_{r_i}\} \\
   q_{i, R} &= \sum_{k = 0}^{l_{r_i}} Q_k I[r_I(k) = c(j + k)] \forall r_i \in \mathbf{R}, j = \text{pos}(r_i | R) \\
   q_i &= \min(q_{i, R}, \min_{j \in \{0, \dots, l_c - l_{r_i}\}} q_{i, j}) \\
   q_c &= \sum_{r_i \in \mathbf{r}} q_i

In the above equation, `r(i)` denotes the base at position
`i` of sequence `r`, and :math:`l_r` denotes the length of
sequence `r`. We pick the consensus sequence that minimizes the
:math:`q_c` value. If the chosen consensus has a log-odds ratio (LOD)
that is greater than `5.0` with respect to the reference, we
realign the reads. This is done by recomputing the CIGAR and MDTag for
each new alignment. Realigned reads have their mapping quality score
increased by 10 in the Phred scale.

