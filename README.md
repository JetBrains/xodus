#JetBrains Xodus

div>
  <a href="http://teamcity.jetbrains.com/viewType.html?buildTypeId=Xodus_Build&guest=1">
    <img src="http://teamcity.jetbrains.com/app/rest/builds/buildType:(id:Xodus_Build)/statusIcon"/>
  </a>
  <span>Build<span>
</div>

##Overview
JetBrains Xodus is a transactional schema-less embedded database written in pure Java. It was initially developed for [JetBrains YouTrack](http://jetbrains.com/youtrack) (issue tracking and project management tool). At the moment Xodus is also used in some internal JetBrains projects.

Key features:
- Xodus is written in pure Java.
- Xodus is transactional and fully ACID-compliant.
- Xodus is highly concurrent. Reads are completely non-blocking due to [MVCC](https://en.wikipedia.org/wiki/Multiversion_concurrency_control) and
true [snapshot isolation](https://en.wikipedia.org/wiki/Snapshot_isolation).
- Xodus is schemaless and agile. It requires no schema migrations or refactorings.
- Xodus is embedded. It doesn’t require installation and administration.

Xodus is free and licensed under [Apache 2.0](http://www.apache.org/licenses/LICENSE-2.0.html). Xodus 1.0-SNAPSHOT artifacts are available in [Maven Central](https://oss.sonatype.org/content/repositories/snapshots/org/jetbrains/xodus) repository.

##Building from Source
[Gradle](http://www.gradle.org) is used to build, test and deploy. To run tests and assemble jars:

    >gradle build

To assemble jars and skip running tests:

    >gradle assemble   

## Find out More
- [Xodus wiki](https://github.com/JetBrains/xodus/wiki)
- [Check out the latest (nightly) builds](https://teamcity.jetbrains.com/viewType.html?buildTypeId=Xodus_Build)
- [Report an issue](http://xodus.myjetbrains.com/youtrack)