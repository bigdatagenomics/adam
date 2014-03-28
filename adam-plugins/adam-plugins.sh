#!/bin/bash

java -classpath "target/adam-plugins-0.6.1-SNAPSHOT.jar:../adam-cli/target/adam-0.6.1-SNAPSHOT.jar" edu.berkeley.cs.amplab.adam.cli.AdamMain "$@"

