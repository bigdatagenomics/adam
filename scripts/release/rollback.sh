#!/bin/sh

mvn -P distribution release:rollback

mvn -P distribution,scala-2.11 release:rollback
