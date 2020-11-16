# <a href="https://github.com/JetBrains/xodus/wiki"><img src="https://raw.githubusercontent.com/wiki/jetbrains/xodus/xodus.png" width=160></a>

[![official JetBrains project](http://jb.gg/badges/official.svg)](https://confluence.jetbrains.com/display/ALL/JetBrains+on+GitHub)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.jetbrains.xodus/xodus-openAPI/badge.svg)](http://search.maven.org/#search%7Cga%7C1%7Corg.jetbrains.xodus%20-dnq%20-time)
[![Last Release](https://img.shields.io/github/release-date/jetbrains/xodus.svg?logo=github)](https://github.com/jetbrains/xodus/releases/latest)
[![TeamCity (build status)](https://img.shields.io/teamcity/http/teamcity.jetbrains.com/s/Xodus_Build.svg)](https://teamcity.jetbrains.com/viewType.html?buildTypeId=Xodus_Build&branch_Xodus=<default>&guest=1)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)
![Pure Java + Kotlin](https://img.shields.io/badge/100%25-java%2bkotlin-orange.svg)
[![Stack Overflow](http://img.shields.io/:stack%20overflow-xodus-brightgreen.svg)](http://stackoverflow.com/questions/tagged/xodus)

JetBrains Xodus is a transactional schema-less embedded database that is written in Java and [Kotlin](https://kotlinlang.org).
It was initially developed for [JetBrains YouTrack](http://jetbrains.com/youtrack), an issue tracking and project
management tool. Xodus is also used in [JetBrains Hub](https://jetbrains.com/hub), the user management platform
for JetBrains' team tools, and in some internal JetBrains projects.

- Xodus is transactional and fully ACID-compliant.
- Xodus is highly concurrent. Reads are completely non-blocking due to [MVCC](https://en.wikipedia.org/wiki/Multiversion_concurrency_control) and
true [snapshot isolation](https://en.wikipedia.org/wiki/Snapshot_isolation).
- Xodus is schema-less and agile. It does not require schema migrations or refactorings.
- Xodus is embedded. It does not require installation or administration.
- Xodus is written in pure Java and [Kotlin](https://kotlinlang.org).
- Xodus is free and licensed under [Apache 2.0](http://www.apache.org/licenses/LICENSE-2.0.html).

## Hello Worlds!

To start using Xodus, define dependencies:
```xml
<!-- in Maven project -->
<dependency>
    <groupId>org.jetbrains.xodus</groupId>
    <artifactId>xodus-openAPI</artifactId>
    <version>1.3.232</version>
</dependency>
```
```groovy
// in Gradle project
dependencies {
    compile 'org.jetbrains.xodus:xodus-openAPI:1.3.232'
}
```
Read more about [managing dependencies](https://github.com/JetBrains/xodus/wiki/Managing-Dependencies).

There are three different ways to deal with data, which results in three different API layers: [Environments](https://github.com/JetBrains/xodus/wiki/Environments), [Entity Stores](https://github.com/JetBrains/xodus/wiki/Entity-Stores) and [Virtual File Systems](https://github.com/JetBrains/xodus/wiki/Virtual-File-Systems).
 
### Environments

Add dependency on `org.jetbrains.xodus:xodus-environment:1.3.232`.

```java
try (Environment env = Environments.newInstance("/home/me/.myAppData")) {
    env.executeInTransaction(txn -> {
        final Store store = env.openStore("Messages", StoreConfig.WITHOUT_DUPLICATES, txn);
        store.put(txn, StringBinding.stringToEntry("Hello"), StringBinding.stringToEntry("World!"));
    });
}
```
### Entity Stores

Add dependency on `org.jetbrains.xodus:xodus-entity-store:1.3.232`, `org.jetbrains.xodus:xodus-environment:1.3.232` and `org.jetbrains.xodus:xodus-vfs:1.3.232`.

```java
try (PersistentEntityStore entityStore = PersistentEntityStores.newInstance("/home/me/.myAppData")) {
    entityStore.executeInTransaction(txn -> {
        final Entity message = txn.newEntity("Message");
        message.setProperty("hello", "World!");
    });
}
```
### Virtual File Systems

Add dependency on `org.jetbrains.xodus:xodus-vfs:1.3.232` and `org.jetbrains.xodus:xodus-environment:1.3.232`.

```java
try (Environment env = Environments.newInstance("/home/me/.myAppData")) {
    final VirtualFileSystem vfs = new VirtualFileSystem(env);
    env.executeInTransaction(txn -> {
        final File file = vfs.createFile(txn, "Messages");
        try (DataOutputStream output = new DataOutputStream(vfs.writeFile(txn, file))) {
            output.writeUTF("Hello ");
            output.writeUTF("World!");
        } catch (IOException e) {
            throw new ExodusException(e);
        }
    });
    vfs.shutdown();
}
```

## Building from Source
[Gradle](http://www.gradle.org) is used to build, test, and publish. JDK 1.8 or higher is required. To build the project, run:

    ./gradlew build

To assemble JARs and skip running tests, run:

    ./gradlew assemble

## Find out More
- [Xodus wiki](https://github.com/JetBrains/xodus/wiki)
- [Report an issue](https://youtrack.jetbrains.com/issues/XD)
- [Stack Overflow](http://stackoverflow.com/questions/tagged/xodus)
- [Xodus-DNQ: data definition and queries Kotlin DSL over Xodus](https://github.com/JetBrains/xodus-dnq)
- [EntityStore browser](https://github.com/JetBrains/xodus-entity-browser)
- [Check out the latest builds](https://teamcity.jetbrains.com/viewType.html?buildTypeId=Xodus_Build)
