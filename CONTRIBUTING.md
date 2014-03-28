How to contribute to ADAM
=========================

Thank you for sharing your code with the ADAM project. We appreciate your contribution!

## Join the mailing list and our IRC channel

If you're not already on the ADAM developers list, [take a minute to join](http://bigdatagenomics.github.io/mail/).
It would be great if you'd introduce yourself to the group but it's not required. You can just
let your code do the talking for you if you like.

You can find us on Freenode IRC in the #adamdev room.

## Check the issue tracker

Before you write too much code, check the [open issues in the ADAM issue tracker](https://github.com/bigdatagenomics/adam/issues?state=open)
to see if someone else has already filed an issue related to your work or is already working on it. If not, go ahead and 
[open a new issue](https://github.com/bigdatagenomics/adam/issues/new).

The issue tracker has a list of ["pick me up!"](https://github.com/bigdatagenomics/adam/issues?labels=pick+me+up%21&page=1&state=open) issues
that are good tasks to help you get familiar with ADAM; they don't require an understanding of genomics and are fairly
limited in scope.

## Announce your work on the mailing list

Shoot us a quick email on the mailing list letting us know what you're working on. There
will likely be people on the list who can give you tips about where to find relevant 
source or alert you to other planned changes that might effect your work.

If the work you're proposing makes substantive changes to ADAM, you may be asked to attach a design document
to your issue in the issue tracker. This document should provide a high-level explanation of your design, clearly define the goal
of the new design and explain the expected effects on performance, APIs, etc. This document is meant to save you time
as it allows the team a chance to provide feedback on the proposes changes. It's likely we can help you find a way
to achieve your goals with less work. The document also allows the team to prepare for large changes to the code
base. We welcome change but also want to ensure that code quality is kept high.

## Submit your pull request

Github provides a nice [overview on how to create a pull request](https://help.github.com/articles/creating-a-pull-request).

Some general rules to follow:

* Do your work in [a fork](https://help.github.com/articles/fork-a-repo) of the ADAM repo.
* Create a branch for each feature/bug in ADAM that you're working on. These branches are often called "feature"
or "topic" branches.
* Use your feature branch in the pull request. Any changes that you push to your feature branch will automatically
be shown in the pull request.
* If your pull request fixes an issue, reference the issue so that it will [be closed when your pull request is merged](https://github.com/blog/1506-closing-issues-via-pull-requests)
* Keep your pull requests as small as possible. Large pull requests are hard to review. Try to break up your changes
into self-contained and incremental pull requests, if need be, and reference dependent pull requests, e.g. "This pull
request builds on request #92. Please review #92 first."
* The first line of commit messages should be a short (<80 character) summary, followed by an empty line and then,
optionally, any details that you want to share about the commit.
* Include unit tests with your pull request. We love tests and [use Jenkins](https://amplab.cs.berkeley.edu/jenkins/)
to check every pull request and commit. Just look for files in the ADAM repo that end in "*Suite.scala", 
e.g. AdamContextSuite.scala, to see examples of how to write tests. You might also want to glance at the 
`./scripts/jenkins-test` script for more end-to-end tests.
* Please try to follow the existing coding style
* Make sure to add a copyright header to all new files (see below).

### Update CHANGES.md file

* The CHANGES.md file is updated to indicate changes that are resolved for the next release.  Please update the file with your commit.

## Copyright header and Licensing

ADAM is released under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html).
Any new files added need to have a header with a license like the following, e.g.

    /**
     * Copyright (c) 2014. [insert your company or name here]
     *
     * Licensed under the Apache License, Version 2.0 (the "License");
     * you may not use this file except in compliance with the License.
     * You may obtain a copy of the License at
     *
     *     http://www.apache.org/licenses/LICENSE-2.0
     *
     * Unless required by applicable law or agreed to in writing, software
     * distributed under the License is distributed on an "AS IS" BASIS,
     * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
     * See the License for the specific language governing permissions and
     * limitations under the License.
     */


