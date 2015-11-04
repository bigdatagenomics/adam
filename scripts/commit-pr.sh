#!/bin/bash

# pick arguments
pr=$1

# do we have enough arguments?
if [ $# != 1 ]; then
    echo "Usage:"
    echo
    echo "./commit-pr.sh <pr-to-commit>"
    exit 1
fi

# get origin urls
fetch_url=$(git remote show origin | grep Fetch | awk '{ print $3 }')
push_url=$(git remote show origin | grep Push | awk '{ print $3 }')


# is origin pointing at the correct repo?
ec=0
if [ $fetch_url != "git@github.com:bigdatagenomics/adam.git" ] && [ $fetch_url != "https://github.com/bigdatagenomics/adam.git" ]
then
    echo "Fetch URL doesn't point at ADAM: ${fetch_url}"
    ec=1
fi
if [ $push_url != "git@github.com:bigdatagenomics/adam.git" ] && [  $push_url != "https://github.com/bigdatagenomics/adam.git" ]
then
    echo "Push URL doesn't point at ADAM: ${push_url}"
    ec=1
fi
if [ $ec != 0 ]; then
    exit 1
fi

# get current branch
branch=$(git status -bs | awk '{ print $2 }' | awk -F'.' '{ print $1 }' | head -n 1)

# are we on master?
if [ $branch != "master" ]; then
    echo "Not on master: $branch"
    exit 1
fi

# fetch latest changes from origin
git fetch origin

# rebase on ToT
git rebase origin/master

# master should be 0 ahead now
numcommits=$(git rev-list --count --left-right master...origin/master | awk '{ sum = $1 + $2 } END { print sum }')
if [ $numcommits != 0 ]; then
    echo "Local master is not 1 commit ahead of remote master..."
    exit 1
fi

# fetch pull request to a branch
git fetch origin pull/${pr}/head:pr-${pr}

# check out this branch
git checkout pr-${pr}

# rebase it on master
git rebase origin/master

# are we one ahead?
numcommits=$(git rev-list --count --left-right pr-${pr}...origin/master  | awk '{ sum = $1 + $2 } END { print sum }')
if [ $numcommits != 1 ]; then
    echo "This PR is ahead by ${numcommits}. Do you want to continue with the merge? y/n"
    read -n 1 -r
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Exiting with commits unmerged on branch pr-${pr}."
        echo "When issues are fixed, you can finish the merge by running:"
        echo
        echo "  git checkout master; git merge --ff-only pr-${pr}; git push origin master"
        echo
        exit 1
    fi
fi

# check out master
git checkout master

# merge branch
git merge --ff-only pr-${pr}

# did merge succeed?
if [ $? == 1 ]; then
    echo "Merge failed with non-zero exit code."
    exit 1
fi

# push to master
git push origin master
