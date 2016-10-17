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

# manually edit and commit changelog changes
#./scripts/changelog.sh $1 | tee CHANGES.md
#git commit -a -m "Modifying changelog."

commit=$(git log --pretty=format:"%H" | head -n 1)
echo "releasing from ${commit} on branch ${branch}"

git push origin ${branch}

# do scala 2.10 release
git checkout -b maint_2.10-${release} ${branch}
mvn --batch-mode \
  -P distribution \
  -Dresume=false \
  -Dtag=adam-parent_2.10-${release} \
  -DreleaseVersion=${release} \
  -DdevelopmentVersion=${devel} \
  -DbranchName=adam_2.10-${release} \
  release:clean \
  release:prepare \
  release:perform

if [ $? != 0 ]; then
  echo "Releasing Spark 1, Scala 2.10 version failed."
  exit 1
fi

# create spark packages zip file
cp adam-cli/target/adam-cli_2.10-${release}.jar adam-${release}.jar
cp adam-cli/pom.xml adam-${release}.pom
zip -r adam-${release}.zip adam-${release}.jar adam-${release}.pom

# do scala 2.11 release
git checkout -b maint_2.11-${release} ${branch}
./scripts/move_to_scala_2.11.sh
git commit -a -m "Modifying pom.xml files for Spark 1, Scala 2.11 release."
mvn --batch-mode \
  -P distribution \
  -Dresume=false \
  -Dtag=adam-parent_2.11-${release} \
  -DreleaseVersion=${release} \
  -DdevelopmentVersion=${devel} \
  -DbranchName=adam_2.11-${release} \
  release:clean \
  release:prepare \
  release:perform

if [ $? != 0 ]; then
  echo "Releasing Spark 1, Scala 2.11 version failed."
  exit 1
fi

# do spark 2, scala 2.10 release
git checkout -b maint_spark2_2.10-${release} ${branch}
./scripts/move_to_spark_2.sh
git commit -a -m "Modifying pom.xml files for Spark 2, Scala 2.10 release."
mvn --batch-mode \
  -P distribution \
  -Dresume=false \
  -Dtag=adam-parent-spark2_2.10-${release} \
  -DreleaseVersion=${release} \
  -DdevelopmentVersion=${devel} \
  -DbranchName=adam-spark2_2.10-${release} \
  release:clean \
  release:prepare \
  release:perform

if [ $? != 0 ]; then
  echo "Releasing Spark 2, Scala 2.10 version failed."
  exit 1
fi

# do spark 2, scala 2.11 release
git checkout -b maint_spark2_2.11-${release} ${branch}
./scripts/move_to_spark_2.sh
./scripts/move_to_scala_2.11.sh
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

if [ $branch = "master" ]; then
  # if original branch was master, update versions on original branch
  git checkout ${branch}
  mvn versions:set -DnewVersion=${devel} \
    -DgenerateBackupPoms=false
  git commit -a -m "Modifying pom.xml files for new development after ${release} release."
  git push origin ${branch}
fi
