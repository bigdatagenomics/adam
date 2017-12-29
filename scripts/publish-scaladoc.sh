#!/bin/bash

set -x -v -e

version=${1:-latest}

# clone repo
git clone -b gh-pages git@github.com:bigdatagenomics/adam.git adam-docs

# get current sha1
sha1=$(git log -1 --pretty=format:%H)

# generate scaladoc
mvn generate-sources scala:doc

# get current scaladoc dir
scaladoc=${PWD}/adam-core/target/site/scaladocs/

# make directory to copy docs to, if it does not already exist
mkdir -p adam-docs/${version}

# copy docs over
cp -rvf ${scaladoc} adam-docs/${version}/scaladocs

# step into repo
cd adam-docs

# mark for add and commit
if [[ ${version} == "latest" ]];
then
    git commit --author "Big Data Genomics <adam-developers@googlegroups.com>" \
        -a -m "Updating docs for ADAM commit ${sha1}."
else
    
    # insert new version html into index.html
    mv index.html index.old
    head -n 17 index.old > index.html
    echo "      <li>${version}:" >> index.html
    echo "        <a href=\"${version}/scaladocs/index.html\">Scaladoc</a>" >> index.html
    echo "      </li>" >> index.html
    tail -n +18 index.old >> index.html
    rm index.old

    git add index.html ${version}
    git commit -m "Adding docs for ADAM ${version} release."
fi    
git push origin gh-pages

# remove adam docs clone
cd ..
rm -rf adam-docs
