# Developer Info

## Unit and Integration Testing in ADAM

For unit testing, ADAM uses [scalatest](http://www.scalatest.org), which is integrated into our
build. If you run `mvn test`, Maven will build the source for the project, build the source for the
tests, and then run all tests. If you want to run a subset of tests, you can specify the tests to run
via a flag. For example, running `mvn test -Dsuites="org.bdgenomics.adam.cli.Features2ADAMSuite"`
will only run the tests in Features2ADAMSuite. For more info on the flags you can pass to the build,
see the [docs](http://www.scalatest.org/user_guide/using_the_scalatest_maven_plugin) for the
Scalatest Maven plugin. `mvn package` will run all tests by default as well, since running `package`
pulls in the `test` lifecycle target.

All of ADAM’s integration tests require pulling data over a network, so we've tagged integration
tests with the `NetworkConnected` [tag](https://github.com/bigdatagenomics/adam/blob/master/adam-core/src/test/scala/org/bdgenomics/adam/util/TestTags.scala).
We don’t run these by default as part of `mvn package` or `mvn test`. However, these tests should be
run as part of the CI builds. To run the network connected tests, you can invoke the
`networkConnected` profile: `mvn -P networkConnected test`.

When writing unit/integration tests, you should extend either `FunSuite` or `SparkFunSuite`. If your
tests will need to run a Spark job, you should extend `SparkFunSuite`. `SparkFunSuite` provides a
SparkContext `sc` that can be used in your tests. _However_, this SparkContext can only be used
inside of a `sparkTest`. If you write a `sparkTest` instead of a `test`, the `SparkFunSuite` will
handle the proper teardown and setup of the SparkContext between tests. If you write a `test` in a
`SparkFunSuite`, the `sc` object will be null. This is useful if you are writing a test suite that
contains a mix of tests that create/don't create Spark jobs.

## Build Infrastructure

All commits to the ADAM trunk and pull requests against ADAM are built on a continuous integration
system, which is maintained at the UC Berkeley campus. We use [Jenkins](http://www.jenkins-ci.org)
for continuous integration. We maintain a pull request builder for ADAM, as well as a builder for
the trunk. The pull request builds whenever a new PR is opened, a PR is updated, or a build is
requested (people registered with the server can request a new build via commenting "Jenkins,
test/retest this please." on the PR). The trunk builder kicks off one daily build, a build whenever
a commit is pushed to bigdatagenomics/adam/master, or whenever a commit is pushed to an upstream
project ([bigdatagenomics/bdg-formats](https://www.github.com/bigdatagenomics/bdg-formats) is the
only upstream dependency that triggers a build). Whenever an ADAM trunk build passes, Jenkins
pushes a snapshot release of ADAM to the [Sonatype](oss.sonatype.org) snapshot servers, and kicks
off builds for the trunks of all downstream BDG projects.
