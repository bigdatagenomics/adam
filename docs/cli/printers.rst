Printing tools
--------------

The printing tools provide some form of user readable view of an ADAM
file. These commands are useful for both quality control and debugging.

print
~~~~~

Dumps a Parquet file to either the console or a text file as
`JSON <http://www.json.org>`__. Takes one required argument:

1. ``FILE(S)``: The file paths to load. These must be Parquet formatted
   files.

This command has several options:

-  ``-pretty``: Pretty prints the JSON output.
-  ``-o``: Provides a path to save the output dump to, instead of
   writing the output to the console.

This command does not support Parquet output, so the only `default
options <#default-args>`__ that this command supports is
``-print_metrics``. Note ``-print_metrics`` is deprecated in ADAM version
0.31.0 and will be removed in version 0.32.0.

flagstat
~~~~~~~~

Runs the ADAM equivalent to the
`SAMTools <http://www.htslib.org/doc/samtools.html>`__ ``flagstat``
command. Takes one required argument:

1. ``INPUT``: The input path. A file containing reads in any of the
   supported ADAM read input formats.

This command has several options:

-  ``-stringency``: Sets the validation stringency for various
   operations. Defaults to ``SILENT.`` See `validation
   stringency <#validation>`__ for more details.
-  ``-o``: Provides a path to save the output dump to, instead of
   writing the output to the console.

This command does not support Parquet output, so the only `default
options <#default-args>`__ that this command supports is
``-print_metrics``. Note ``-print_metrics`` is deprecated in ADAM version
0.31.0 and will be removed in version 0.32.0.

view
~~~~

Runs the ADAM equivalent to the
`SAMTools <http://www.htslib.org/doc/samtools.html>`__ ``view`` command.
Takes one required argument:

1. ``INPUT``: The input path. A file containing reads in any of the
   supported ADAM read input formats.

In addition to the `default options <#default-args>`__, this command
supports the following options:

-  ``-o``: Provides a path to save the output dump to, instead of
   writing the output to the console. Format is autodetected as any of
   the ADAM read outputs.
-  ``-F``/``-f``: Filters reads that either match all (``-f``) or none
   (``-F``) of the flag bits.
-  ``-G``/``-g``: Filters reads that either mismatch all (``-g``) or
   none (``-G``) of the flag bits.
-  ``-c``: Prints the number of reads that (mis)matched the filters,
   instead of the reads themselves. Conflicts with ``-o``.

