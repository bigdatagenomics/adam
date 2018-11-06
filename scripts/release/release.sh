#!/bin/sh

# do we have enough arguments?
if [ $# < 4 ]; then
    echo "Usage:"
    echo
    echo "./release.sh <release version> <development version> <milestone id>"
    exit 1
fi

# pick arguments
release=$1
devel=$2
milestone=$3

# get current branch
branch=$(git status -bs | awk '{ print $2 }' | awk -F'.' '{ print $1 }' | head -n 1)

# update changelog per Github milestone
mvn com.github.heuermh.maven.plugin.changes:github-changes-maven-plugin:1.0:github-changes -DmilestoneId=${milestone}
git commit -a -m "Modifying changelog."

commit=$(git log --pretty=format:"%H" | head -n 1)
echo "releasing from ${commit} on branch ${branch}"

git push origin ${branch}

# do spark 2, scala 2.11 release
git checkout -b maint_spark2_2.11-${release} ${branch}

cd adam-python
virtualenv adam-release
. adam-release/bin/activate
make develop
deactivate
rm -rf adam-release
cd ..

git commit -a -m "Modifying pom.xml files for Spark 2, Scala 2.11 release."
mvn --batch-mode \
  -P distribution \
  -Dresume=false \
  -Dtag=adam-parent-spark2_2.11-${release} \
  -DreleaseVersion=${release} \
  -DdevelopmentVersion=${devel} \
  -DbranchName=adam-spark2_2.11-${release} \
  release:clean \
  release:prepare \
  release:perform

if [ $? != 0 ]; then
  echo "Releasing Spark 2, Scala 2.11 version failed."
  exit 1
fi

# set up python environment for releasing to pypi
pushd adam-python
rm -rf release-venv
virtualenv release-venv
. release-venv/bin/activate
pip install pyspark
pip install twine
pip install pypandoc

# clean any possible extant sdists
rm -rf dist

# build sdist and push to pypi
make sdist
twine upload dist/*.tar.gz

# deactivate the python virtualenv
deactivate
rm -rf release-venv

popd

# build R tarball
#
# !!!!!
# NOTE:
# !!!!!
#
# We will not be pushing to CRAN until SparkR is reinstated in CRAN. Until then,
# this tarball will need to be manually attached to the github releases page
pushd adam-r
R CMD build bdgenomics.adam
popd

if [ $? != 0 ]; then
  echo "Releasing bdgenomics.adam to PyPi failed."
  exit 1
fi

# publish docs
./scripts/publish-scaladoc.sh ${release}

if [ $branch = "master" ]; then
  # if original branch was master, update versions on original branch
  git checkout ${branch}
  mvn versions:set -DnewVersion=${devel} \
    -DgenerateBackupPoms=false
  git commit -a -m "Modifying pom.xml files for new development after ${release} release."
  git push origin ${branch}
fi
