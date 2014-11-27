#JetBrains Xodus
JetBrains Xodus is pure Java transactional schemaless embedded database. Initially, it was developed for
[JetBrains YouTrack](http://jetbrains.com/youtrack) bug and issue tracker; at the moment it is used in
some internal JetBrains projects as well.

Key features:
- Xodus is written in pure Java.
- Xodus is transactional and fully ACID-compliant.
- Xodus is highly concurrent. In implements [MVCC](https://en.wikipedia.org/wiki/Multiversion_concurrency_control) and
true [snapshot isolation](https://en.wikipedia.org/wiki/Snapshot_isolation) allow completely non-blocking reads.
- Xodus is schemaless and agile. No schema migrations or refactorings.
- Xodus is embedded and requires no dedicated server. No installation of it, no administration of it, no intercommunication with it.

Xodus 1.0-SNAPSHOT artifacts are available in [Maven Central](https://oss.sonatype.org/content/repositories/snapshots/org/jetbrains/xodus) repository.

[Learn more](https://github.com/JetBrains/xodus/wiki)

[Check out the latest (nightly) builds](https://teamcity.jetbrains.com/viewType.html?buildTypeId=Xodus_Build)

[File an issue or bug](http://xodus.myjetbrains.com/youtrack)
