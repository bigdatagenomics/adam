#!/bin/sh

# do we have enough arguments?
if [ $# < 3 ]; then
    echo "Usage:"
    echo
    echo "./release.sh <release version> <development version>"
    exit 1
fi

# pick arguments
release=$1
devel=$2

# get current branch
branch=$(git status -bs | awk '{ print $2 }' | awk -F'.' '{ print $1 }' | head -n 1)

./scripts/changelog.sh $1 | tee CHANGES.md
git commit -a -m "Modifying changelog."

commit=$(git log --pretty=format:"%H" | head -n 1)
echo "releasing from ${commit} on branch ${branch}"

git push origin ${branch}

# do scala 2.10 release
git checkout -b maint-2.10_${release} ${branch}
mvn --batch-mode \
  -P distribution \
  -Dresume=false \
  -Dtag=adam-parent-2.10_${release} \
  -DreleaseVersion=${release} \
  -DdevelopmentVersion=${devel} \
  -DbranchName=adam-${release} \
  release:clean \
  release:prepare \
  release:perform

if [ $? != 0 ]; then
  echo "Releasing Scala 2.10 version failed."
  exit 1
fi

# do scala 2.11 release
git checkout -b maint-2.11_${release} ${branch}
./scripts/move_to_scala_2.11.sh
git commit -a -m "Modifying pom.xml files for 2.11 release."
mvn --batch-mode \
  -P distribution \
  -Dresume=false \
  -Dtag=adam-parent-2.11_${release} \
  -DreleaseVersion=${release} \
  -DdevelopmentVersion=${devel} \
  -DbranchName=adam-2.11_${release} \
  release:clean \
  release:prepare \
  release:perform

if [ $? != 0 ]; then
  echo "Releasing Scala 2.11 version failed."
  exit 1
fi

if [ $branch = "master" ]; then
  # if original branch was master, update versions on original branch
  git checkout ${branch}
  mvn versions:set -DnewVersion=${devel} \
    -DgenerateBackupPoms=false
  git commit -a -m "Modifying pom.xml files for new development after ${release} release."
  git push origin ${branch}
fi