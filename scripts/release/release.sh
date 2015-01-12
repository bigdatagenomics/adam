#!/bin/sh

mvn -P distribution -Dresume=false release:clean release:prepare release:perform

mvn -P distribution,scala-2.11 -Dresume=false release:clean release:prepare release:perform
