# Building Downstream Applications {#apps}

ADAM is packaged so that it can be used interatively via the ADAM shell, called from
the command line interface (CLI), or included as a library when building downstream
applications.

This document covers three patterns for building applications downstream of ADAM:

* Extend the ADAM CLI by [adding new commands](#commands)
* Extend the ADAM CLI by [adding new commands in an external repository](#external-commands)
* Use ADAM as a [library in new applications](#library)


## Extend the ADAM CLI by adding new commands {#commands}

ADAM's CLI is implemented in the adam-cli Apache Maven module of the
[bdgenomics/adam](https://github.com/bigdatagenomics/adam) repository, one
.scala source file for each CLI action (e.g. [Transform.scala](https://github.com/bigdatagenomics/adam/blob/master/adam-cli/src/main/scala/org/bdgenomics/adam/cli/Transform.scala)
for the [transform](#transform) action), and a main class ([ADAMMain.scala](https://github.com/bigdatagenomics/adam/blob/master/adam-cli/src/main/scala/org/bdgenomics/adam/cli/ADAMMain.scala))
that assembles and delegates to the various CLI actions.

To add a new command:

Extend `Args4jBase` class to specify arguments to the command. Arguments are defined using
the [args4j library](http://args4j.kohsuke.org/). If reading from or writing to Parquet,
consider including Parquet arguments via `with ParquetArgs`.

```scala
class MyCommandArgs extends Args4jBase with ParquetArgs {
  @Argument(required = true, metaVar = "INPUT", usage = "Input to my command", index = 0)
  var inputPath: String = null
}
```

Extend `BDGCommandCompanion` object to specify the command name and description. The `apply`
method associates `MyCommandArgs` defined above with `MyCommand`.

```scala
object MyCommand extends BDGCommandCompanion {
  val commandName = "myCommand"
  val commandDescription = "My command example."

  def apply(cmdLine: Array[String]) = {
    new MyCommand(Args4j[MyCommandArgs](cmdLine))
  }
}
```

Extend `BDGSparkCommand` class and implement the `run(SparkContext)` method. The `MyCommandArgs`
class defined above is provided in the constructor and specifies the generic type for `BDGSparkCommand`.
The companion object defined above is declared as a field.  For access to an
[slf4j](http://www.slf4j.org/) Logger via the `log` field, specify `with Logging`.

```scala
class MyCommand(protected val args: MyCommandArgs) extends BDGSparkCommand[MyCommandArgs] with Logging {
  val companion = MyCommand

  def run(sc: SparkContext) {
    log.info("Doing something...")
    // do something
  }
}
```

Add the new command to the default list of commands in `ADAMMain`.

```scala
  val defaultCommandGroups =
    List(
      CommandGroup(
        "ADAM ACTIONS",
        List(
          MyCommand,
          CountReadKmers,
          CountContigKmers, ...
```

Build ADAM and run the new command via `adam-submit`.

```bash
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
```

Then consider making a pull request to include the new command in ADAM!


## Extend the ADAM CLI by adding new commands in an external repository {#external-commands}

To extend the ADAM CLI by adding new commands in an external repository,
instead of editing `ADAMMain` to add new commands as above, create a new
object with a `main(args: Array[String])` method that delegates to `ADAMMain`
and provides additional command(s) via its constructor.

```scala
import org.bdgenomics.adam.cli.{ ADAMMain, CommandGroup }
import org.bdgenomics.adam.cli.ADAMMain.defaultCommandGroups

object MyCommandsMain {
  def main(args: Array[String]) {
    val commandGroup = List(CommandGroup("MY COMMANDS", List(MyCommand1, MyCommand2)))
    new ADAMMain(defaultCommandGroups.union(commandGroup))(args)
  }
}
```

Build the project and run the new external commands via `adam-submit`,
specifying `ADAM_MAIN` environment variable as the new main class,
and providing the jar file in the Apache Spark `--jars` argument.

Note the `--` argument separator between Apache Spark arguments and
ADAM arguments.

```bash
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
```

A complete example of this pattern can be found in the
[heuermh/adam-commands](https://github.com/heuermh/adam-examples) repository.


## Use ADAM as a library in new applications {#library}

To use ADAM as a library in new applications:

Create an object with a `main(args: Array[String])` method and handle
command line arguments. Feel free to use the [args4j library](http://www.slf4j.org/)
or any other argument parsing library.

```scala
object MyExample {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("at least one argument required, e.g. input.foo")
      System.exit(1)
    }
```

Create an Apache Spark configuration `SparkConf` and use it to create a new `SparkContext`.
The following serialization configuration needs to be present to register ADAM classes. If
any additional [Kyro serializers](https://github.com/EsotericSoftware/kryo) need to be
registered, [create a registrator that delegates to the ADAM registrator](#registrator).
You might want to provide your own serializer registrator if you need custom serializers for
a class in your code that either has a complex structure that Kryo fails to serialize properly
via Kryo's serializer inference, or if you want to require registration of all classes in your
application to improve performance.

```scala
    val conf = new SparkConf()
      .setAppName("MyCommand")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "org.bdgenomics.adam.serialization.ADAMKryoRegistrator")
      .set("spark.kryo.referenceTracking", "true")

    val sc = new SparkContext(conf)
    // do something
```

Configure the new application build to create a fat jar artifact with ADAM and its
transitive dependencies included. For example, this `maven-shade-plugin` configuration
would work for an Apache Maven build.

```xml
<plugin>
  <groupId>org.apache.maven.plugins</groupId>
  <artifactId>maven-shade-plugin</artifactId>
  <configuration>
    <createDependencyReducedPom>false</createDependencyReducedPom>
    <filters>
      <filter>
        <artifact>*:*</artifact>
        <excludes>
          <exclude>META-INF/*.SF</exclude>
          <exclude>META-INF/*.DSA</exclude>
          <exclude>META-INF/*.RSA</exclude>
        </excludes>
      </filter>
    </filters>
  </configuration>
  <executions>
    <execution>
      <phase>package</phase>
      <goals>
        <goal>shade</goal>
      </goals>
      <configuration>
        <transformers>
          <transformer implementation="org.apache.maven.plugins.shade.resource.ServicesResourceTransformer" />
        </transformers>
      </configuration>
    </execution>
  </executions>
</plugin>
```

Build the new application and run via `spark-submit`.

```bash
spark-submit \
  --class MyCommand \
  target/my-command.jar \
  input.foo
```


A complete example of this pattern can be found in the
[heuermh/adam-examples](https://github.com/heuermh/adam-examples) repository.

### Writing your own registrator that calls the ADAM registrator {#registrator}

As we do in ADAM, an application may want to provide its own Kryo serializer
registrator. The custom registrator may be needed in order to register custom
serializers, or because the application's configuration requires all serializers
to be registered. In either case, the application will need to provide its own
Kryo registrator. While this registrator can manually register ADAM's serializers,
it is simpler to call to the ADAM registrator from within the registrator. As an
example, this pattern looks like the following code:

```scala
import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import org.bdgenomics.adam.serialization.ADAMKryoRegistrator

class MyCommandKryoRegistrator extends KryoRegistrator {

  private val akr = new ADAMKryoRegistrator()

  override def registerClasses(kryo: Kryo) {

    // register adam's requirements
    akr.registerClasses(kryo)

    // ... register any other classes I need ...
  }
}
```


