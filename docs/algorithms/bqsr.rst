BQSR Implementation
~~~~~~~~~~~~~~~~~~~

Base quality score recalibration seeks to identify and correct
correlated errors in base quality score estimates. At a high level, this
is done by associating sequenced bases with possible error covariates,
and estimating the true error rate of this covariate. Once the true
error rate of all covariates has been estimated, we then apply the
corrected covariate.

Our system is generic and places no limitation on the number or type of
covariates that can be applied. A covariate describes a parameter space
where variation in the covariate parameter may be correlated with a
sequencing error. We provide two common covariates that map to common
sequencing errors (Nakamura et al. 2011):

-  *CycleCovariate:* This covariate expresses which cycle the base was
   sequenced in. Read errors are known to occur most frequently at the
   start or end of reads.
-  *DinucCovariate:* This covariate covers biases due to the sequence
   context surrounding a site. The two-mer ending at the sequenced base
   is used as the covariate parameter value.

To generate the covariate observation table, we aggregate together the
number of observed and error bases per covariate. The two algorithms
below demonstrate this process.

.. math::

   read &\leftarrow \text{the read to observe} \\
   covariates &\leftarrow \text{covariates to use for recalibration} \\
   sites &\leftarrow \text{sites of known variation} \\
   observations &\leftarrow \emptyset \\
   \text{for} &base \in read \\
   &covariate \leftarrow identifyCovariate(base) \\
   &\text{if isUnknownSNP}(base, sites) \\
   &observation \leftarrow Observation(1, 1) \\
   &\text{else} \\
   &observation \leftarrow Observation(1, 0) \\
   &observations.\text{append}((covariate, observation)) \\
   &\text{return} observations \\

.. math::

   reads &\leftarrow \text{input dataset} \\
   covariates &\leftarrow \text{covariates to use for recalibration} \\
   sites &\leftarrow \text{known variant sites} \\
   sites.\text{broadcast}() \\
   observations &\leftarrow reads.\text{map}(read \Rightarrow \text{emitObservations}(read, covariates, sites)) \\
   table &\leftarrow observations.\text{aggregate}(\text{CovariateTable}(), \text{mergeCovariates}) \\
   \text{return} table

The ``Observation`` class stores the number of bases seen and the number of
errors seen. For example, ``Observation(1, 1)`` creates an
``Observation`` object that has seen one base, which was an erroneous
base.

Once we have computed the observations that correspond to each
covariate, we estimate the observed base quality using the below
equation. This represents a Bayesian model of the mismatch probability
with Binomial likelihood and a Beta(1, 1) prior.

.. math::


   \mathbf{E}(P_{err}|{cov}) = \frac{\text{errors}(cov) + 1}{\text{observations}(cov) + 2}

After these probabilities are estimated, we go back across the input
read dataset and reconstruct the quality scores of the read by using the
covariate assigned to the read to look into the covariate table.

