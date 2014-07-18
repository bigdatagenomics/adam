#!/bin/bash

# get current sha1
sha1=$(git log -1 --pretty=format:%H)

# generate scaladoc
mvn scala:doc

# get current scaladoc dir
scaladoc=${PWD}/adam-core/target/site/scaladocs/

# clone repo
git clone git@github.com:bigdatagenomics/bigdatagenomics.github.io.git

# get maven artifact version
version=$(mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version | grep -v "\[")

# clean and make new dir
bdgscaladocdir=${PWD}/bigdatagenomics.github.io/projects/adam/scaladoc/${version}
rm -rf ${bdgscaladocdir}
mkdir -p ${bdgscaladocdir}

# copy scaladoc over
cp -rv ${scaladoc}/* ${bdgscaladocdir}

# step into repo
cd bigdatagenomics.github.io

# mark for add and commit
git add projects/adam/scaladoc/${version}/*
git commit -m "Adding scaladoc for ADAM commit ${sha1}."
git push origin master

# clean up bdg repo
cd ..
rm -rf bigdatagenomics.github.io