```      
          _________    _____________              _____               
          ______  /______  /___  __ )____________ ___(_)______________
          ___ _  /_  _ \  __/_  __  |_  ___/  __ `/_  /__  __ \_  ___/
          / /_/ / /  __/ /_ _  /_/ /_  /   / /_/ /_  / _  / / /(__  ) 
          \____/  \___/\__/ /_____/ /_/    \__,_/ /_/  /_/ /_//____/  
                                                                      
                      ____  __     _________              
                      __  |/ /___________  /___  _________
                      __    /_  __ \  __  /_  / / /_  ___/
                      _    | / /_/ / /_/ / / /_/ /_(__  ) 
                      /_/|_| \____/\__,_/  \__,_/ /____/    
```

<div>
  <a href="http://teamcity.jetbrains.com/viewType.html?buildTypeId=Xodus_Build&guest=1">
    <img src="http://teamcity.jetbrains.com/app/rest/builds/buildType:(id:Xodus_Build)/statusIcon"/>
  </a>
</div>

##Overview
JetBrains Xodus is a transactional schema-less embedded database written in pure Java. It was initially developed for [JetBrains YouTrack](http://jetbrains.com/youtrack) (an issue tracking and project management tool). Currently Xodus is also used in [JetBrains Hub](https://jetbrains.com/hub) (JetBrains' team tools connector) and in some internal JetBrains projects.

Key features:
- Xodus is written in pure Java.
- Xodus is transactional and fully ACID-compliant.
- Xodus is highly concurrent. Reads are completely non-blocking due to [MVCC](https://en.wikipedia.org/wiki/Multiversion_concurrency_control) and
true [snapshot isolation](https://en.wikipedia.org/wiki/Snapshot_isolation).
- Xodus is schema-less and agile. It requires no schema migrations or refactorings.
- Xodus is embedded. It doesn’t require installation or administration.

Xodus is free and licensed under [Apache 2.0](http://www.apache.org/licenses/LICENSE-2.0.html). Xodus 1.0-SNAPSHOT artifacts are available in [Sonartype OSS](https://oss.sonatype.org/content/repositories/snapshots/org/jetbrains/xodus) repository.

##Building from Source
[Gradle](http://www.gradle.org) is used to build, test and deploy. To run tests and assemble jars:

    >gradle build

To assemble jars and skip running tests:

    >gradle assemble   

## Find out More
- [Xodus wiki](https://github.com/JetBrains/xodus/wiki)
- [Check out the latest (nightly) builds](https://teamcity.jetbrains.com/viewType.html?buildTypeId=Xodus_Build)
- [Report an issue](http://xodus.myjetbrains.com/youtrack)
- <a href="mailto:xodus-feedback@jetbrains.com">Ask questions by e-mail</a>