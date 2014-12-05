#!/bin/sh

mvn -P distribution -Dresume=false release:clean release:prepare release:perform
