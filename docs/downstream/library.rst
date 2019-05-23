Use ADAM as a library in new applications
-----------------------------------------

To use ADAM as a library in new applications:

Create an object with a ``main(args: Array[String])`` method and handle
command line arguments. Feel free to use the `args4j
library <https://args4j.kohsuke.org>`__ or any other argument parsing
library.

.. code:: scala

    object MyExample {
      def main(args: Array[String]) {
        if (args.length < 1) {
          System.err.println("at least one argument required, e.g. input.foo")
          System.exit(1)
        }
      }
    }

Create an Apache Spark configuration ``SparkConf`` and use it to create
a new ``SparkContext``. The following serialization configuration needs
to be present to register ADAM classes. If any additional `Kyro
serializers <https://github.com/EsotericSoftware/kryo>`__ need to be
registered, `create a registrator that delegates to the ADAM
registrator <#registrator>`__. You might want to provide your own
serializer registrator if you need custom serializers for a class in
your code that either has a complex structure that Kryo fails to
serialize properly via Kryo's serializer inference, or if you want to
require registration of all classes in your application to improve
performance.

.. code:: scala

        val conf = new SparkConf()
          .setAppName("MyCommand")
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .set("spark.kryo.registrator", "org.bdgenomics.adam.serialization.ADAMKryoRegistrator")
          .set("spark.kryo.referenceTracking", "true")

        val sc = new SparkContext(conf)
        // do something

Configure the new application build to create a fat jar artifact with
ADAM and its transitive dependencies included. For example, this
``maven-shade-plugin`` configuration would work for an Apache Maven
build.

.. code:: xml

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

Build the new application and run via ``spark-submit``.

.. code:: bash

    spark-submit \
      --class MyCommand \
      target/my-command.jar \
      input.foo

A complete example of this pattern can be found in the
`heuermh/adam-examples <https://github.com/heuermh/adam-examples>`__
repository.

Writing your own registrator that calls the ADAM registrator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

As we do in ADAM, an application may want to provide its own Kryo
serializer registrator. The custom registrator may be needed in order to
register custom serializers, or because the application's configuration
requires all serializers to be registered. In either case, the
application will need to provide its own Kryo registrator. While this
registrator can manually register ADAM's serializers, it is simpler to
call to the ADAM registrator from within the registrator. As an example,
this pattern looks like the following code:

.. code:: scala

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

