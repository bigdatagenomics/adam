The bdg-formats schemas
-----------------------

The schemas that comprise ADAM's narrow waist are defined in the
`bdg-formats <https://github.com/bigdatagenomics/bdg-formats>`__
project, using the `Apache Avro <https://avro.apache.org>`__ schema
description language. This schema definition language automatically
generates implementations of this schema for multiple common languages,
including Java, C, C++, and Python. bdg-formats contains several core
schemas:

-  The *Alignment* schema represents a genomic read, along with
   that read's alignment to a reference genome, if available.
-  The *Feature* schema represents a generic genomic feature. This
   record can be used to tag a region of the genome with an annotation,
   such as coverage observed over that region, or the coordinates of an
   exon.
-  The *Fragment* schema represents a set of read alignments that came
   from a single sequenced fragment.
-  The *Genotype* schema represents a genotype call, along with
   annotations about the quality/read support of the called genotype.
-  The *Sequence* and *Slice* schema represents sequences and slices of
   sequences, respectfully.
-  The *Variant* schema represents a sequence variant, along with
   statistics about that variant's support across a group of samples,
   and annotations about the effect of the variant.

The bdg-formats schemas are designed so that common fields are easy to
query, while maintaining extensibility and the ability to interoperate
with common genomics file formats. Where necessary, the bdg-formats
schemas are nested, which allows for the description of complex nested
features and groupings (such as the Fragment record, which groups
together Alignments). All fields in the bdg-formats schemas are
nullable, and the schemas themselves do not contain invariants around
valid values for a field. Instead, we validate data on ingress and
egress to/from a conventional genomic file format. This allows users to
take advantage of features such as field projection, which can improve
the performance of queries like `flagstat <#flagstat>`__ by an order of
magnitude.
