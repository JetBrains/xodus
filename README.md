# JetBrains Xodus

[![official JetBrains project](http://jb.gg/badges/official.svg)](https://confluence.jetbrains.com/display/ALL/JetBrains+on+GitHub)
[![TeamCity (build status)](https://img.shields.io/teamcity/http/teamcity.jetbrains.com/s/Xodus_Build.svg)](http://teamcity.jetbrains.com/viewType.html?buildTypeId=Xodus_Build&guest=1)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.jetbrains.xodus/xodus-openAPI/badge.svg)](http://search.maven.org/#search%7Cga%7C1%7Corg.jetbrains.xodus)
[![GitHub license](https://img.shields.io/hexpm/l/plug.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)
![Repository Size](https://reposs.herokuapp.com/?path=JetBrains/xodus)
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
    <version>1.0.6</version>
</dependency>
```
```groovy
// in Gradle project
dependencies {
    compile 'org.jetbrains.xodus:xodus-openAPI:1.0.6'
}
```
Read more about [managing dependencies](https://github.com/JetBrains/xodus/wiki/Managing-Dependencies).

There are three different ways to deal with data, which results in three different API layers: [Environments](https://github.com/JetBrains/xodus/wiki/Environments), [Entity Stores](https://github.com/JetBrains/xodus/wiki/Entity-Stores) and [Virtual File Systems](https://github.com/JetBrains/xodus/wiki/Virtual-File-Systems).
 
### Environments
```java
final Environment env = Environments.newInstance("/home/me/.myAppData");
env.executeInTransaction(new TransactionalExecutable() {
    @Override
    public void execute(@NotNull final Transaction txn) {
        final Store store = env.openStore("Messages", StoreConfig.WITHOUT_DUPLICATES, txn)
        store.put(txn, StringBinding.stringToEntry("Hello"), StringBinding.stringToEntry("World!"));
    }
});
env.close();
```
### Entity Stores
```java
final PersistentEntityStore entityStore = PersistentEntityStores.newInstance("/home/me/.myAppData");
entityStore.executeInTransaction(new StoreTransactionalExecutable() {
    @Override
    public void execute(@NotNull final StoreTransaction txn) {
        final Entity message = txn.newEntity("Message");
        message.setProperty("hello", "World!");
    }
});
entityStore.close();
```
### Virtual File Systems
```java
final Environment env = Environments.newInstance("/home/me/.myAppData");
final VirtualFileSystem vfs = new VirtualFileSystem(env);
env.executeInTransaction(new TransactionalExecutable() {
    @Override
    public void execute(@NotNull final Transaction txn) {
        final File file = vfs.createFile(txn, "Messages");
        try (DataOutputStream output = new DataOutputStream(vfs.writeFile(txn, file))) {
            output.writeUTF("Hello ");
            output.writeUTF("World!");
        }
    }
});
vfs.shutdown();
env.close(); 
```

## Building from Source
[Gradle](http://www.gradle.org) is used to build, test, and publish. JDK 1.8 is required. To build the project, run:

    ./gradlew build

To assemble JARs and skip running tests, run:

    ./gradlew assemble

## Find out More
- [Xodus wiki](https://github.com/JetBrains/xodus/wiki)
- [Report an issue](https://youtrack.jetbrains.com/issues/XD)
- [Stack Overflow](http://stackoverflow.com/questions/tagged/xodus)
- [Check out the latest builds](https://teamcity.jetbrains.com/viewType.html?buildTypeId=Xodus_Build)
- [Observe development in Upsource](https://upsource.jetbrains.com/Xodus/view)
- [EntityStore browser](https://github.com/lehvolk/xodus-entity-browser)
- <a href="mailto:xodus-feedback@jetbrains.com">Ask questions by e-mail</a>
