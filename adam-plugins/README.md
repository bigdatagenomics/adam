adam-plugins
============

An example of an implementation of AdamPlugin external to the main ADAM build.


###hacking adam-plugins

First build/install adam

    $ mvn install


Then build/package adam-plugins

    $ cd adam-plugins
    $ mvn package


An example bash script shows how to add the adam-plugin jar to the classpath.

    $ ./adam-plugin.sh


Run the external plugin

    $ ./adam-plugins.sh plugin \
           edu.berkeley.cs.amplab.adam.plugins.external.ExternalTake10Plugin \
           ../adam-core/src/test/resources/small.sam
