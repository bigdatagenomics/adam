Notes for release managers
---

This document describes how to make an ADAM release.

Setup your environment
1. Copy (or incorporate) the settings.xml file to ```~/.m2/settings.xml```
2. Request the ADAM packager private GPG key
3. Edit the username, password, etc in ```~/.m2/settings.xml```

Once your environment is setup, you'll be able to do a release.

Then from the project root directory, run `./scripts/release/release.sh`.  This script requires
parameters for release version, next development version, and release Github Milestone identifier, e.g.

```bash
$ ./scripts/release/release.sh 0.27.0 0.28.0-SNAPSHOT 21
```

Once you've successfully published the release, you will need to "close" and "release" it following the instructions at
http://central.sonatype.org/pages/releasing-the-deployment.html#close-and-drop-or-release-your-staging-repository

After the release is rsynced to the Maven Central repository, confirm checksums match and verify signatures.

The ADAM Homebrew formula and Bioconda recipe may need updating, if the auto-update bots are not able to pick up the release.

Finally, be sure to announce the release on the ADAM mailing list, Gitter, Big Data Genomics blog, and Twitter (@bigdatagenomics).
