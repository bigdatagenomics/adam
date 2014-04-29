#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$DIR/.."
VERSION=$(grep "<version>" "$PROJECT_ROOT/pom.xml"  | head -2 | tail -1 | sed 's/ *<version>//g' | sed 's/<\/version>//g')
ADAM_JAR=$PROJECT_ROOT/adam-cli/target/adam-$VERSION.jar

SPARK_CLASSPATH=$ADAM_JAR spark-shell -i:$DIR/scala-console.scala
