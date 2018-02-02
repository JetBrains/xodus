# <img src="https://raw.githubusercontent.com/wiki/jetbrains/xodus/xodus.png" width=160>

The [VirtualFileSystem](https://github.com/JetBrains/xodus/blob/master/vfs/src/main/java/jetbrains/exodus/vfs/VirtualFileSystem.java) lets you deal with data in terms of [files](https://github.com/JetBrains/xodus/blob/master/vfs/src/main/java/jetbrains/exodus/vfs/File.java), input, and output streams. `VirtualFileSystem` works over an [Environment](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/env/Environment.java) instance:
```java
final Environment env = Environments.newInstance("/home/me/.myAppData");
final VirtualFileSystem vfs = new VirtualFileSystem(env);
```

Any `VirtualFileSystem` operation requires a [Transaction](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/env/Transaction.java) instance. An application working with `VirtualFileSystem` should [shutdown()](https://github.com/JetBrains/xodus/blob/master/vfs/src/main/java/jetbrains/exodus/vfs/VirtualFileSystem.java#L591) it before closing the underlying `Environment`:
```java
vfs.shutdown();
env.close();
```

## Files

In the following examples, we assume that all operations are performed inside a [Transaction](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/env/Transaction.java) named `txn`. To create a [file](https://github.com/JetBrains/xodus/blob/master/vfs/src/main/java/jetbrains/exodus/vfs/File.java):
```java
final File file = vfs.createFile(txn, "myFile");
```
`"myFile"` is the _path_ of the file. This method throws `FileExistsException` on an attempt to create a file with the path of any other existing file. The path is an abstract string and can be used to codify a hierarchy, though `VirtualFileSystem` doesn't contain methods to enumerate files by a path prefix. 

The file does not appear in the file system until the transaction is flushed or committed. You can also create a file with the `openFile()` method:
```java
final File file = vfs.openFile(txn, "myFile", true);
```
The Boolean parameter `true` lets you create the file if it does not exist in the file system.

You can create a file with a unique auto-generated path and specified path prefix with [createUniqueFile()](https://github.com/JetBrains/xodus/blob/master/vfs/src/main/java/jetbrains/exodus/vfs/VirtualFileSystem.java#L245).

In addition to the path, any `File` gets a unique auto-generated [file descriptor](https://github.com/JetBrains/xodus/blob/master/vfs/src/main/java/jetbrains/exodus/vfs/File.java#L84). This can be used further on a par with the path to identify the file.

## Streams

To access file contents, `VirtualFileSystem` lets you create an `InputStream` and `OutputStream` that are associated with the file.
```java
// read the file from the beginning
final InputStream input = vfs.readFile(txn, file);
// read the file from specified position
final long position = 31415926;
final InputStream input = vfs.readFile(txn, file, position);
// write the file from the beginning
final OutputStream output = vfs.writeFile(txn, file);
// write the file from specified position
final OutputStream output = vfs.writeFile(txn, file, position);
// write the file from the end of the file (append file)
final OutputStream output = vfs.appendFile(txn, file);
```

## Lucene Directory

[ExodusDirectory](https://github.com/JetBrains/xodus/blob/master/lucene-directory/src/main/java/jetbrains/exodus/lucene/ExodusDirectory.java) is a good sample of using `VirtualFileSystem`. It implements `org.apache.lucene.store.Directory` and stores the contents of a full-text index that was created by [Apache Lucene](http://lucene.apache.org) in Xodus. See the [tests](https://github.com/JetBrains/xodus/tree/master/lucene-directory/src/test/java/jetbrains/exodus/lucene) to find out how `ExodusDirectory` can be used.

To use `ExodusDirectory` in your application, define a dependency on the `xodus-lucene-directory` artifacts:
```xml
<!-- in Maven project -->
<dependency>
    <groupId>org.jetbrains.xodus</groupId>
    <artifactId>xodus-lucene-directory</artifactId>
    <version>1.0.0</version>
</dependency>
```
```groovy
// in Gradle project
dependencies {
    compile 'org.jetbrains.xodus:xodus-lucene-directory:1.0.0'
}
```