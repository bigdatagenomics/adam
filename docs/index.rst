ADAM User Guide
===============

Introduction
============

ADAM is a library and command line tool that enables the use of `Apache
Spark <https://spark.apache.org>`__ to parallelize genomic data analysis
across cluster/cloud computing environments. ADAM uses a set of schemas
to describe genomic sequences, reads, variants/genotypes, and features,
and can be used with data in legacy genomic file formats such as
SAM/BAM/CRAM, BED/GFF3/GTF, and VCF, as well as data stored in the
columnar `Apache Parquet <https://parquet.apache.org>`__ format. On a
single node, ADAM provides competitive performance to optimized
multi-threaded tools, while enabling scale out to clusters with more
than a thousand cores. ADAM's APIs can be used from Scala, Java, Python,
R, and SQL.

The ADAM/Big Data Genomics Ecosystem
------------------------------------

ADAM builds upon the open source `Apache
Spark <https://spark.apache.org>`__, `Apache
Avro <https://avro.apache.org>`__, and `Apache
Parquet <https://parquet.apache.org>`__ projects. Additionally, ADAM can
be deployed for both interactive and production workflows using a
variety of platforms. A diagram of the ecosystem of tools and libraries
that ADAM builds on and the tools that build upon the ADAM APIs can be
found below.

.. figure:: img/bdgenomics-stack.png
   :alt: The ADAM ecosystem.

   The ADAM ecosystem.

As the diagram shows, beyond the `ADAM CLI <#cli>`__, there are a number
of tools built using ADAM's core APIs:

-  `Avocado <https://github.com/bigdatagenomics/avocado>`__ is a variant
   caller built on top of ADAM for germline and somatic calling
-  `Cannoli <https://github.com/bigdatagenomics/cannoli>`__ uses ADAM's
   `pipe <#pipes>`__ API to parallelize common single-node genomics
   tools (e.g., `BWA <https://github.com/lh3/bwa>`__,
   `bowtie2 <http://bowtie-bio.sourceforge.net/bowtie2/index.shtml>`__,
   `FreeBayes <https://github.com/ekg/freebayes>`__)
-  `DECA <https://github.com/bigdatagenomics/deca>`__ is a
   reimplementation of the XHMM copy number variant caller on top of
   ADAM/Apache Spark
-  `Gnocchi <https://github.com/bigdatagenomics/gnocchi>`__ provides
   primitives for running GWAS/eQTL tests on large genotype/phenotype
   datasets using ADAM
-  `Lime <https://github.com/bigdatagenomics/lime>`__ provides a
   parallel implementation of genomic set theoretic primitives using the
   `region join API <#join>`__
-  `Mango <https://github.com/bigdatagenomics/mango>`__ is a library for
   visualizing large scale genomics data with interactive latencies and
   serving data using the `GA4GH
   schemas <https://github.com/ga4gh/schemas>`__

.. toctree::
   :caption: Architecture
   :maxdepth: 2

   architecture/overview
   architecture/stackModel
   architecture/schemas
   architecture/evidence

.. toctree::
   :caption: Installation
   :maxdepth: 2

   installation/source
   installation/pip
   installation/example

.. toctree::
   :caption: Benchmarking
   :maxdepth: 2

   benchmarks/algorithms
   benchmarks/storage

.. toctree::
   :caption: Deploying ADAM
   :maxdepth: 2

   deploying/cgcloud
   deploying/yarn
   deploying/toil
   deploying/slurm

.. toctree::
   :caption: The ADAM CLI
   :maxdepth: 2

   cli/overview
   cli/actions
   cli/conversions
   cli/printers

.. toctree::
   :caption: ADAM's APIs
   :maxdepth: 2

   api/overview
   api/adamContext
   api/genomicRdd
   api/joins
   api/pipes

.. toctree::
   :caption: Building Downstream Applications
   :maxdepth: 2

   downstream/overview
   downstream/cli
   downstream/library

.. toctree::
   :caption: Algorithms in ADAM
   :maxdepth: 2

   algorithms/reads
   algorithms/bqsr
   algorithms/ri
   algorithms/dm
   algorithms/joins

* :ref:`genindex`
* :ref:`search`

References
==========
  
.. raw:: html

   <div id="references" class="references">

.. raw:: html

   <div id="ref-armbrust15">

Armbrust, Michael, Reynold S. Xin, Cheng Lian, Yin Huai, Davies Liu,
Joseph K. Bradley, Xiangrui Meng, et al. 2015. "Spark SQL: Relational
Data Processing in Spark." In *Proceedings of the International
Conference on Management of Data (SIGMOD '15)*.

.. raw:: html

   </div>

.. raw:: html

   <div id="ref-depristo11">

DePristo, Mark A, Eric Banks, Ryan Poplin, Kiran V Garimella, Jared R
Maguire, Christopher Hartl, Anthony A Philippakis, et al. 2011. "A
Framework for Variation Discovery and Genotyping Using Next-Generation
DNA Sequencing Data." *Nature Genetics* 43 (5). Nature Publishing Group:
491–98.

.. raw:: html

   </div>

.. raw:: html

   <div id="ref-langmead09crossbow">

Langmead, Ben, Michael C Schatz, Jimmy Lin, Mihai Pop, and Steven L
Salzberg. 2009. "Searching for SNPs with Cloud Computing." *Genome
Biology* 10 (11). BioMed Central: R134.

.. raw:: html

   </div>

.. raw:: html

   <div id="ref-li10">

Li, Heng, and Richard Durbin. 2010. "Fast and Accurate Long-Read
Alignment with Burrows-Wheeler Transform." *Bioinformatics* 26 (5).
Oxford Univ Press: 589–95.

.. raw:: html

   </div>

.. raw:: html

   <div id="ref-massie13">

Massie, Matt, Frank Nothaft, Christopher Hartl, Christos Kozanitis,
André Schumacher, Anthony D Joseph, and David A Patterson. 2013. "ADAM:
Genomics Formats and Processing Patterns for Cloud Scale Computing."
UCB/EECS-2013-207, EECS Department, University of California, Berkeley.

.. raw:: html

   </div>

.. raw:: html

   <div id="ref-mckenna10">

McKenna, Aaron, Matthew Hanna, Eric Banks, Andrey Sivachenko, Kristian
Cibulskis, Andrew Kernytsky, Kiran Garimella, et al. 2010. "The Genome
Analysis Toolkit: A MapReduce Framework for Analyzing Next-Generation
DNA Sequencing Data." *Genome Research* 20 (9). Cold Spring Harbor Lab:
1297–1303.

.. raw:: html

   </div>

.. raw:: html

   <div id="ref-melnik10">

Melnik, Sergey, Andrey Gubarev, Jing Jing Long, Geoffrey Romer, Shiva
Shivakumar, Matt Tolton, and Theo Vassilakis. 2010. "Dremel: Interactive
Analysis of Web-Scale Datasets." *Proceedings of the VLDB Endowment* 3
(1-2). VLDB Endowment: 330–39.

.. raw:: html

   </div>

.. raw:: html

   <div id="ref-nakamura11">

Nakamura, Kensuke, Taku Oshima, Takuya Morimoto, Shun Ikeda, Hirofumi
Yoshikawa, Yuh Shiwa, Shu Ishikawa, et al. 2011. "Sequence-Specific
Error Profile of Illumina Sequencers." *Nucleic Acids Research*. Oxford
Univ Press, gkr344.

.. raw:: html

   </div>

.. raw:: html

   <div id="ref-nothaft15">

Nothaft, Frank A, Matt Massie, Timothy Danford, Zhao Zhang, Uri
Laserson, Carl Yeksigian, Jey Kottalam, et al. 2015. "Rethinking
Data-Intensive Science Using Scalable Analytics Systems." In
*Proceedings of the 2015 ACM SIGMOD International Conference on
Management of Data (SIGMOD ’15)*. ACM.

.. raw:: html

   </div>

.. raw:: html

   <div id="ref-sandberg85">

Sandberg, Russel, David Goldberg, Steve Kleiman, Dan Walsh, and Bob
Lyon. 1985. "Design and Implementation of the Sun Network Filesystem."
In *Proceedings of the USENIX Conference*, 119–30.

.. raw:: html

   </div>

.. raw:: html

   <div id="ref-schadt10">

Schadt, Eric E, Michael D Linderman, Jon Sorenson, Lawrence Lee, and
Garry P Nolan. 2010. "Computational Solutions to Large-Scale Data
Management and Analysis." *Nature Reviews Genetics* 11 (9). Nature
Publishing Group: 647–57.

.. raw:: html

   </div>

.. raw:: html

   <div id="ref-schatz09">

Schatz, Michael C. 2009. "CloudBurst: Highly Sensitive Read Mapping with
MapReduce." *Bioinformatics* 25 (11). Oxford Univ Press: 1363–69.

.. raw:: html

   </div>

.. raw:: html

   <div id="ref-sherry01">

Sherry, Stephen T, M-H Ward, M Kholodov, J Baker, Lon Phan, Elizabeth M
Smigielski, and Karl Sirotkin. 2001. "dbSNP: The NCBI Database of
Genetic Variation." *Nucleic Acids Research* 29 (1). Oxford Univ Press:
308–11.

.. raw:: html

   </div>

.. raw:: html

   <div id="ref-smith81">

Smith, Temple F, and Michael S Waterman. 1981. "Identification of Common
Molecular Subsequences." *Journal of Molecular Biology* 147 (1).
Elsevier: 195–97.

.. raw:: html

   </div>

.. raw:: html

   <div id="ref-picard">

The Broad Institute of Harvard and MIT. 2014. "Picard."
http://broadinstitute.github.io/picard/.

.. raw:: html

   </div>

.. raw:: html

   <div id="ref-vavilapalli13">

Vavilapalli, Vinod Kumar, Arun C Murthy, Chris Douglas, Sharad Agarwal,
Mahadev Konar, Robert Evans, Thomas Graves, et al. 2013. "Apache Hadoop
YARN: Yet Another Resource Negotiator." In *Proceedings of the Symposium
on Cloud Computing (SoCC '13)*, 5. ACM.

.. raw:: html

   </div>

.. raw:: html

   <div id="ref-vivian16">

Vivian, John, Arjun Rao, Frank Austin Nothaft, Christopher Ketchum, Joel
Armstrong, Adam Novak, Jacob Pfeil, et al. 2016. "Rapid and Efficient
Analysis of 20,000 RNA-Seq Samples with Toil." *BioRxiv*. Cold Spring
Harbor Labs Journals.

.. raw:: html

   </div>

.. raw:: html

   <div id="ref-zaharia12">

Zaharia, Matei, Mosharaf Chowdhury, Tathagata Das, Ankur Dave, Justin
Ma, Murphy McCauley, Michael Franklin, Scott Shenker, and Ion Stoica.
2012. "Resilient Distributed Datasets: A Fault-Tolerant Abstraction for
in-Memory Cluster Computing." In *Proceedings of the Conference on
Networked Systems Design and Implementation (NSDI ’12)*, 2. USENIX
Association.

.. raw:: html

   </div>

.. raw:: html

   <div id="ref-zimmermann80">

Zimmermann, Hubert. 1980. "OSI Reference Model–The ISO Model of
Architecture for Open Systems Interconnection." *IEEE Transactions on
Communications* 28 (4). IEEE: 425–32.

.. raw:: html

   </div>

.. raw:: html

   </div>


