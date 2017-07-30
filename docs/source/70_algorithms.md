# Core Algorithms {#algorithms}

## Read Preprocessing Algorithms

In ADAM, we have implemented the three most-commonly used pre-processing stages
from the GATK pipeline [@depristo11]. In this section, we describe the stages
that we have implemented, and the techniques we have used to improve 
performance and accuracy when running on a distributed system. These
pre-processing stages include:

* *Duplicate Removal:* During the process of preparing DNA for sequencing, 
reads are duplicated by errors during the sample preparation and polymerase
chain reaction stages. Detection of duplicate reads requires matching all reads
by their position and orientation after read alignment. Reads with identical
position and orientation are assumed to be duplicates. When a group of
duplicate reads is found, each read is scored, and all but the highest quality
read are marked as duplicates. We have validated our duplicate removal code
against Picard [@picard], which is used by the GATK for Marking Duplicates. Our
implementation is fully concordant with the Picard/GATK duplicate removal
engine, except we are able to perform duplicate marking for chimeric read
pairs.[^1] Specifically, because Picard's traversal engine is restricted to 
processing linearly sorted alignments, Picard mishandles these alignments.
Since our engine is not constrained by the underlying layout of data on disk,
we are able to properly handle chimeric read pairs.
* *Local Realignment:* In local realignment, we correct areas where variant
alleles cause reads to be locally misaligned from the reference genome.[^2] In
this algorithm, we first identify regions as targets for realignment. In the
GATK, this identification is done by traversing sorted read alignments. In our
implementation, we fold over partitions where we generate targets, and then we
merge the tree of targets. This process allows us to eliminate the data shuffle
needed to achieve the sorted ordering. As part of this fold, we must compute
the convex hull of overlapping regions in parallel. We discuss this in more
detail later in this section. After we have generated the targets, we associate
reads to the overlapping target, if one exists. After associating reads to
realignment targets, we run a heuristic realignment algorithm that works by
minimizing the quality-score weighted number of bases that mismatch against the
reference.
* *Base Quality Score Recalibration (BQSR):* During the sequencing process,
systemic errors occur that lead to the incorrect assignment of base quality
scores. In this step, we label each base that we have sequenced with an error
covariate. For each covariate, we count the total number of bases that we saw,
as well as the total number of bases within the covariate that do not match the
reference genome. From this data, we apply a correction by estimating the error
probability for each set of covariates under a beta-binomial model with uniform
prior. We have validated the concordance of our BQSR implementation against the
GATK. Across both tools, only 5000 of the 180B bases ($<0.0001\%$) in the
high-coverage NA12878 genome dataset differ. After investigating this
discrepancy, we have determined that this is due to an error in the GATK, where
paired-end reads are mishandled if the two reads in the pair overlap.
* *ShuffleRegionJoin Load Balancing:* Because of the non-uniform distribution
of regions in mapped reads, joining two genomic datasets can be difficult or
impossible when neither dataset fits completely on a single node. To reduce the
impact of data skew on the runtime of joins, we implemented a load balancing
engine in ADAM’s ShuffleRegionJoin core. This load balancing is a preprocessing
step to the ShuffleRegionJoin and improves performance by 10--100x. The first
phase of the load balancer is to sort and repartition the left dataset evenly
across all partitions, regardless of the mapped region. This offers
significantly better distribution of the data than the standard binning
approach. After rebalancing the data, we copartition the right dataset with the
left based on the region bounds of each partition. Once the data has been 
copartitioned, it is sorted locally and the join is performed.



[^1]: In a chimeric read pair, the two reads in the read pairs align to
different chromosomes; see Li et al [@li10].
[^2]: This is typically caused by the presence of insertion/deletion (INDEL)
variants; see DePristo et al [@depristo11].

In the rest of this section, we discuss the high level implementations of these
algorithms.

### BQSR Implementation {#bqsr}

Base quality score recalibration seeks to identify and correct correlated
errors in base quality score estimates. At a high level, this is done by 
associating sequenced bases with possible error covariates, and estimating the
true error rate of this covariate. Once the true error rate of all covariates
has been estimated, we then apply the corrected covariate.

Our system is generic and places no limitation on the number or type of
covariates that can be applied. A covariate describes a parameter space where
variation in the covariate parameter may be correlated with a sequencing error.
We provide two common covariates that map to common sequencing errors
[@nakamura11]:

* *CycleCovariate:* This covariate expresses which cycle the base was sequenced
in. Read errors are known to occur most frequently at the start or end of
reads.
* *DinucCovariate:* This covariate covers biases due to the sequence context
surrounding a site. The two-mer ending at the sequenced base is used as the
covariate parameter value.

To generate the covariate observation table, we aggregate together the number
of observed and error bases per covariate. Algorithms 
\ref{alg:emit-observations} and \ref{alg:create-table} demonstrate this
process.

\begin{algorithm}
\caption{Emit Observed Covariates}
\label{alg:emit-observations}
\begin{algorithmic}
\STATE $read \leftarrow$ the read to observe
\STATE $covariates \leftarrow$ covariates to use for recalibration
\STATE $sites \leftarrow$ sites of known variation
\STATE $observations \leftarrow \emptyset$
\FOR{$base \in read$}
\STATE $covariate \leftarrow$ identifyCovariate($base$)
\IF{isUnknownSNP($base, sites$)}
\STATE $observation \leftarrow$ Observation($1, 1$)
\ELSE
\STATE $observation \leftarrow$ Observation($1, 0$)
\ENDIF
\STATE $observations$.append($(covariate, observation)$)
\ENDFOR
\RETURN $observations$
\end{algorithmic}
\end{algorithm}

\begin{algorithm}
\caption{Create Covariate Table}
\label{alg:create-table}
\begin{algorithmic}
\STATE $reads \leftarrow$ input dataset
\STATE $covariates \leftarrow$ covariates to use for recalibration
\STATE $sites \leftarrow$ known variant sites
\STATE $sites$.broadcast()
\STATE $observations \leftarrow reads$.map($read \Rightarrow$ emitObservations($read, covariates, sites$))
\STATE $table \leftarrow$ $observations$.aggregate(CovariateTable(), mergeCovariates)
\RETURN $table$
\end{algorithmic}
\end{algorithm}

In Algorithm \ref{alg:emit-observations}, the `Observation` class stores the
number of bases seen and the number of errors seen. For example,
`Observation(1, 1)` creates an `Observation` object that has seen one base,
which was an erroneous base.

Once we have computed the observations that correspond to each covariate, we
estimate the observed base quality using the below equation. This represents a
Bayesian model of the mismatch probability with Binomial likelihood and a
Beta(1, 1) prior.

$$
\mathbf{E}(P_{err}|{cov}) = \frac{\text{\#errors}(cov) + 1}{\text{\#observations}(cov) + 2}
$$

After these probabilities are estimated, we go back across the input read
dataset and reconstruct the quality scores of the read by using the covariate
assigned to the read to look into the covariate table.

### Indel Realignment Implementation {#realignment}

Although global alignment will frequently succeed at aligning reads to the
proper region of the genome, the local alignment of the read may be incorrect.
Specifically, the error models used by aligners may penalize local alignments
containing INDELs more than a local alignment that converts the alignment to a
series of mismatches. To correct for this, we perform local realignment of the
reads against consensus sequences in a three step process. In the first step,
we identify candidate sites that have evidence of an insertion or deletion. We
then compute the convex hull of these candidate sites, to determine the windows
we need to realign over. After these regions are identified, we generate
candidate haplotype sequences, and realign reads to minimize the overall
quantity of mismatches in the region.

#### Realignment Target Identification

To identify target regions for realignment, we simply map across all the reads.
If a read contains INDEL evidence, we then emit a region corresponding to the
region covered by that read.

#### Convex-Hull Finding

Once we have identified the target realignment regions, we must then find the
maximal convex hulls across the set of regions. For a set $R$ of regions, we
define a maximal convex hull as the largest region $\hat{r}$ that satisfies the
following properties:

\begin{align}
\hat{r} &= \cup_{r_i \in \hat{R}} r_i \\
\hat{r} \cap r_i &\ne \emptyset, \forall r_i \in \hat{R} \\
\hat{R} &\subset R
\end{align}

In our problem, we seek to find all of the maximal convex hulls, given a set of
regions. For genomics, the convexity constraint described by equation
\eqref{eqn:convexity-constraint} is trivial to check: specifically, the genome
is assembled out of reference contigs that define disparate 1-D coordinate
spaces. If two regions exist on different contigs, they are known not to
overlap. If two regions are on a single contig, we simply check to see if they
overlap on that contig's 1-D coordinate plane.

Given this realization, we can define Algorithm \ref{alg:parallel-convex-hull},
which is a data parallel algorithm for finding the maximal convex hulls that
describe a genomic dataset.

\begin{algorithm}
\caption{Find Convex Hulls in Parallel}
\label{alg:parallel-convex-hull}
\begin{algorithmic}
\STATE $data \leftarrow$ input dataset
\STATE $regions \leftarrow data$.map($data \Rightarrow $generateTarget($data$))
\STATE $regions \leftarrow regions$.sort()
\STATE $hulls \leftarrow regions$.fold($r_1, r_2 \Rightarrow$ mergeTargetSets($r_1, r_2$))
\RETURN $hulls$
\end{algorithmic}
\end{algorithm}

The `generateTarget` function projects each datapoint into a Red-Black tree
that contains a single region. The performance of the fold depends on the
efficiency of the merge function. We achieve efficient merges with the
tail-call recursive `mergeTargetSets` function that is described in Algorithm
\ref{alg:join-targets}.

\begin{algorithm}
\caption{Merge Hull Sets}
\label{alg:join-targets}
\begin{algorithmic}
\STATE $first \leftarrow$ first target set to merge
\STATE $second \leftarrow$ second target set to merge
\REQUIRE $first$ and $second$ are sorted
\IF{$first = \emptyset \wedge second = \emptyset$}
\RETURN $\emptyset$
\ELSIF{$first = \emptyset$}
\RETURN $second$
\ELSIF{$second = \emptyset$}
\RETURN $first$
\ELSE
\IF{last($first$) $\cap$ head($second$) $= \emptyset$}
\RETURN $first$ + $second$
\ELSE
\STATE $mergeItem \leftarrow$ (last($first$) $\cup$ head($second$))
\STATE $mergeSet \leftarrow$ allButLast($first$) $\cup mergeItem$
\STATE $trimSecond \leftarrow$ allButFirst($second$)
\RETURN mergeTargetSets($mergeSet$, $trimSecond$)
\ENDIF
\ENDIF
\end{algorithmic}
\end{algorithm}

The set returned by this function is used as an index for mapping reads
directly to realignment targets.

#### Candidate Generation and Realignment {#consensus-model}

Once we have generated the target set, we map across all the reads and check to
see if the read overlaps a realignment target. We then group together all reads
that map to a given realignment target; reads that do not map to a target are
randomly assigned to a ``null'' target. We do not attempt realignment for reads
mapped to null targets.

To process non-null targets, we must first generate candidate haplotypes to
realign against. We support several processes for generating these consensus
sequences:

* *Use known INDELs:* Here, we use known variants that were provided by the
user to generate consensus sequences. These are typically derived from a source
of common variants such as dbSNP [@sherry01].
* *Generate consensuses from reads:* In this process, we take all INDELs that
are contained in the alignment of a read in this target region.
* *Generate consensuses using Smith-Waterman:* With this method, we take all
reads that were aligned in the region and perform an exact Smith-Waterman
alignment [@smith81] against the reference in this site. We then take the
INDELs that were observed in these realignments as possible consensuses. 

From these consensuses, we generate new haplotypes by inserting the INDEL
consensus into the reference sequence of the region. Per haplotype, we then
take each read and compute the quality score weighted Hamming edit distance of
the read placed at each site in the consensus sequence. We then take the
minimum quality score weighted edit versus the consensus sequence and the
reference genome. We aggregate these scores together for all reads against this
consensus sequence. Given a consensus sequence $c$, a reference sequence $R$,
and a set of reads $\mathbf{r}$, we calculate this score using the equation 
below.

\begin{align}
q_{i, j} &= \sum_{k = 0}^{l_{r_i}} Q_k I[r_I(k) = c(j + k)] \forall r_i \in \mathbf{R}, j \in \{0, \dots, l_c - l_{r_i}\} \\
q_{i, R} &= \sum_{k = 0}^{l_{r_i}} Q_k I[r_I(k) = c(j + k)] \forall r_i \in \mathbf{R}, j = \text{pos}(r_i | R) \\
q_i &= \min(q_{i, R}, \min_{j \in \{0, \dots, l_c - l_{r_i}\}} q_{i, j}) \\
q_c &= \sum_{r_i \in \mathbf{r}} q_i
\end{align}

In the above equation, $s(i)$ denotes the base at position $i$ of sequence $s$,
and $l_s$ denotes the length of sequence $s$. We pick the consensus sequence
that minimizes the $q_c$ value. If the chosen consensus has a log-odds ratio
(LOD) that is greater than $5.0$ with respect to the reference, we realign the
reads. This is done by recomputing the CIGAR and MDTag for each new alignment.
Realigned reads have their mapping quality score increased by 10 in the Phred
scale.

### Duplicate Marking Implementation {#duplicate-marking}

Reads may be duplicated during sequencing, either due to clonal duplication
via PCR before sequencing, or due to optical duplication while on the
sequencer. To identify duplicated reads, we apply a heuristic algorithm that
looks at read fragments that have a consistent mapping signature. First, we 
bucket together reads that are from the same sequenced fragment by grouping
reads together on the basis of read name and record group. Per read bucket, we
then identify the 5' mapping positions of the primarily aligned reads. We mark
as duplicates all read pairs that have the same pair alignment locations, and
all unpaired reads that map to the same sites. Only the highest scoring
read/read pair is kept, where the score is the sum of all quality scores in the
read that are greater than 15.

### ShuffleRegionJoin Load Balancing

ShuffleRegionJoins perform a sort-merge join on distributed genomic data. The
current standard for distributing genomic data are to use a binning approach
where ranges of genomic data are assigned to a particular partition. This
approach has a significant limitation that we aim to solve: no matter how
fine-grained the bins created, they can never resolve extremely skewed data.
ShuffleRegionJoin also requires that the data be sorted, so we keep track of
the fact that  knowledge of sort through the join so we can reuse this
knowledge downstream.

The first step in ShuffleRegionJoin is to sort and balance the data. This is
done with a sampling method and the data are sorted if it was not previously.
When we shuffle the data, we also store the region ranges for all the data on
this partition. Storing these partition bounds allows us to copartition the
right dataset by assigning all records to a partition if the record falls
within the partition bounds. After the right data are colocated with the
correct records in the left dataset, we perform the join locally on each
partition.

Maintaining the sorted knowledge and partition bounds are extremely useful for
downstream applications that can take advantage of sorted data. Subsequent
joins, for example, will be much faster because the data are already relatively
balanced and sorted. Additional set theory and aggregation primitives, such as
counting nearby regions, grouping and clustering nearby regions, and finding
the set difference will all benefit from the sorted knowledge because each of
these primitives requires that the data be sorted first.
