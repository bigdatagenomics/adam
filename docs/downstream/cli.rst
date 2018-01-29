Extend the ADAM CLI by adding new commands
------------------------------------------

ADAM's CLI is implemented in the adam-cli Apache Maven module of the
`bdgenomics/adam <https://github.com/bigdatagenomics/adam>`__
repository, one .scala source file for each CLI action (e.g.
`TransformAlignments.scala <https://github.com/bigdatagenomics/adam/blob/master/adam-cli/src/main/scala/org/bdgenomics/adam/cli/TransformAlignments.scala>`__
for the `transformAlignments <#transform-alignments>`__ action), and a
main class
(`ADAMMain.scala <https://github.com/bigdatagenomics/adam/blob/master/adam-cli/src/main/scala/org/bdgenomics/adam/cli/ADAMMain.scala>`__)
that assembles and delegates to the various CLI actions.

To add a new command:

1. `Extend Args4jBase to specify arguments <#extend-args4jbase-to-specify-arguments>`__
2. `Extend BDGCommandCompanion <#extend-bdgcommandcompanion>`__
3. `Build ADAM and run the new command <#extend-the-adam-cli-by-adding-new-commands-in-an-external-repository>`__

Extend Args4jBase to specify arguments
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Extend ``Args4jBase`` class to specify arguments to your new command.
Arguments are defined using the `args4j
library <http://args4j.kohsuke.org/>`__. If reading from or writing to
Parquet, consider including Parquet arguments via ``with ParquetArgs``.

.. code:: scala

    class MyCommandArgs extends Args4jBase with ParquetArgs {
      @Argument(required = true, metaVar = "INPUT", usage = "Input to my command", index = 0)
      var inputPath: String = null
    }

Extend BDGCommandCompanion
~~~~~~~~~~~~~~~~~~~~~~~~~~

Extend ``BDGCommandCompanion`` object to specify the command name and
description. The ``apply`` method associates ``MyCommandArgs`` defined
above with ``MyCommand``.

.. code:: scala

    object MyCommand extends BDGCommandCompanion {
      val commandName = "myCommand"
      val commandDescription = "My command example."

      def apply(cmdLine: Array[String]) = {
        new MyCommand(Args4j[MyCommandArgs](cmdLine))
      }
    }

Extend ``BDGSparkCommand`` class and implement the ``run(SparkContext)``
method. The ``MyCommandArgs`` class defined above is provided in the
constructor and specifies the CLI command for ``BDGSparkCommand``. We
must define a companion object because the command cannot run without
being added to the list of accepted commands described below. For access
to an `slf4j <http://www.slf4j.org/>`__ Logger via the ``log`` field,
mix in the org.bdgenomics.utils.misc.Logging trait by adding
``with Logging`` to the class definition.

.. code:: scala

    class MyCommand(protected val args: MyCommandArgs) extends BDGSparkCommand[MyCommandArgs] with Logging {
      val companion = MyCommand

      def run(sc: SparkContext) {
        log.info("Doing something...")
        // do something
      }
    }

Add the new command to the default list of commands in
org.bdgenomics.adam.cli.ADAMMain.

.. code:: scala

    ...
      val defaultCommandGroups =
        List(
          CommandGroup(
            "ADAM ACTIONS",
            List(
              MyCommand,
              CountReadKmers,
              CountContigKmers,
    ...

Build ADAM and run the new command
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Build ADAM and run the new command via ``adam-submit``.

::

    $ mvn install
    $ ./bin/adam-submit --help
    Using ADAM_MAIN=org.bdgenomics.adam.cli.ADAMMain
    Using SPARK_SUBMIT=/usr/local/bin/spark-submit

           e         888~-_          e             e    e
          d8b        888   \        d8b           d8b  d8b
         /Y88b       888    |      /Y88b         d888bdY88b
        /  Y88b      888    |     /  Y88b       / Y88Y Y888b
       /____Y88b     888   /     /____Y88b     /   YY   Y888b
      /      Y88b    888_-~     /      Y88b   /          Y888b

    Usage: adam-submit [<spark-args> --] <adam-args>

    Choose one of the following commands:

    ADAM ACTIONS
               myCommand : My command example.
              countKmers : Counts the k-mers/q-mers from a read dataset.
        countContigKmers : Counts the k-mers/q-mers from a read dataset.
    ...

    $ ./bin/adam-submit myCommand input.foo

Then consider creating an
`issue <https://github.com/bigdatagenomics/adam/issues/new>`__ to start
the process toward including the new command in ADAM! Please see
`CONTRIBUTING.md <https://github.com/bigdatagenomics/adam/blob/master/CONTRIBUTING.md>`__
before opening a new `pull
request <https://github.com/bigdatagenomics/adam/compare?expand=1>`__.

Extend the ADAM CLI by adding new commands in an external repository
--------------------------------------------------------------------

To extend the ADAM CLI by adding new commands in an external repository,
create a new object with a ``main(args: Array[String])`` method that
delegates to ``ADAMMain`` and provides additional command(s) via its
constructor.

.. code:: scala

    import org.bdgenomics.adam.cli.{ ADAMMain, CommandGroup }
    import org.bdgenomics.adam.cli.ADAMMain.defaultCommandGroups

    object MyCommandsMain {
      def main(args: Array[String]) {
        val commandGroup = List(CommandGroup("MY COMMANDS", List(MyCommand1, MyCommand2)))
        new ADAMMain(defaultCommandGroups.union(commandGroup))(args)
      }
    }

Build the project and run the new external commands via ``adam-submit``,
specifying ``ADAM_MAIN`` environment variable as the new main class, and
providing the jar file in the Apache Spark ``--jars`` argument.

Note the ``--`` argument separator between Apache Spark arguments and
ADAM arguments.

::

    $ ADAM_MAIN=MyCommandsMain \
      adam-submit \
      --jars my-commands.jar \
      -- \
      --help

    Using ADAM_MAIN=MyCommandsMain
    Using SPARK_SUBMIT=/usr/local/bin/spark-submit

           e         888~-_          e             e    e
          d8b        888   \        d8b           d8b  d8b
         /Y88b       888    |      /Y88b         d888bdY88b
        /  Y88b      888    |     /  Y88b       / Y88Y Y888b
       /____Y88b     888   /     /____Y88b     /   YY   Y888b
      /      Y88b    888_-~     /      Y88b   /          Y888b

    Usage: adam-submit [<spark-args> --] <adam-args>

    Choose one of the following commands:
    ...

    MY COMMANDS
              myCommand1 : My command example 1.
              myCommand2 : My command example 2.

    $ ADAM_MAIN=MyCommandsMain \
      adam-submit \
      --jars my-commands.jar \
      -- \
      myCommand1 input.foo

A complete example of this pattern can be found in the
`heuermh/adam-commands <https://github.com/heuermh/adam-examples>`__
repository.

