# Build Errors

_The build cannot continue because of the following unsatisfied dependency: - maven:dom4j:1.7-20060614:jar_

Upgrade your maven install to latest version (3.x).

On MacOS you can use `brew`, e.g. `brew install maven`

These will *not* work:

```bash
sudo port install mvn` or `sudo port install mvn1
```


_`[ERROR] Java heap space -> [Help 1]`_
_`...`_
_`[ERROR] [Help 1] http://cwiki.apache.org/confluence/display/MAVEN/OutOfMemoryError`_

On Mac OS X, run this and/or put in your ~/.bashrc or ~/.profile file:
```bash
export MAVEN_OPTS="-Xmx512m -XX:MaxPermSize=128m"
```
