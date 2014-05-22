#!/bin/sh

mvn -Dresume=false release:clean release:prepare release:perform
