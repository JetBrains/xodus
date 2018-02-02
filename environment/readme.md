# <img src="https://raw.githubusercontent.com/wiki/jetbrains/xodus/xodus.png" width=160>

The **Environments** layer provides the lowest-level API available. [Environment](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/env/Environment.java) encapsulates one or more [stores](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/env/Store.java) that contain data. This encapsulation lets you perform read and modify operations against multiple stores within a single [transaction](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/env/Transaction.java).

In short, [Environment](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/env/Environment.java) is a transactional key-value storage. [Store](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/env/Store.java) is a named collection of key/value pairs. If a `Store` is allowed to contain duplicate keys, then it is a map. Otherwise, it is a multi-map. Also, `Store` can be thought as a table with two columns, one for keys and another for values. Both keys and values are managed using instances of [ByteIterable](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/ByteIterable.java). You can use [cursors](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/env/Cursor.java) to iterate over a `Store`, for example, to find the nearest key or key/value pair. All operations can only be performed within a transaction. The API and implementation live in the _jetbrains.exodus.env_ package.

[Data Environments](https://github.com/JetBrains/xodus/wiki/Environments#data-environments)
<br>[Transactions](https://github.com/JetBrains/xodus/wiki/Environments#transactions)
<br>[ByteIterables](https://github.com/JetBrains/xodus/wiki/Environments#byteiterables)
<br>[Bindings](https://github.com/JetBrains/xodus/wiki/Environments#bindings)
<br>[Stores](https://github.com/JetBrains/xodus/wiki/Environments#stores)
<br>&nbsp;&nbsp;&nbsp;&nbsp;[Writing](https://github.com/JetBrains/xodus/wiki/Environments#writing)
<br>&nbsp;&nbsp;&nbsp;&nbsp;[Reading](https://github.com/JetBrains/xodus/wiki/Environments#reading)
<br>&nbsp;&nbsp;&nbsp;&nbsp;[Deleting](https://github.com/JetBrains/xodus/wiki/Environments#deleting)
<br>&nbsp;&nbsp;&nbsp;&nbsp;[ContextualStore](https://github.com/JetBrains/xodus/wiki/Environments#contextualstore)
<br>[Cursors](https://github.com/JetBrains/xodus/wiki/Environments#cursors)
<br>&nbsp;&nbsp;&nbsp;&nbsp;[Traversing key/value Pairs](https://github.com/JetBrains/xodus/wiki/Environments#traversing-keyvalue-pairs)
<br>&nbsp;&nbsp;&nbsp;&nbsp;[Navigating to Keys and Values](https://github.com/JetBrains/xodus/wiki/Environments#navigating-to-keys-and-values)
<br>&nbsp;&nbsp;&nbsp;&nbsp;[Deleting key/value Pairs](https://github.com/JetBrains/xodus/wiki/Environments#deleting-keyvalue-pairs)
<br>&nbsp;&nbsp;&nbsp;&nbsp;[Range Search](https://github.com/JetBrains/xodus/wiki/Environments#range-search)

## Data Environments

To open an Environment, create an instance of `Environment` with the help of the [Environments](https://github.com/JetBrains/xodus/blob/master/environment/src/main/java/jetbrains/exodus/env/Environments.java) utility class.
```java
Environment env = Environments.newInstance("/home/me/.myAppData");
```
This method opens an existing database or creates a new database in the directory that is passed as a parameter. It is not possible to share a single database directory between different environments. Any attempt to do this (from within any process, current or not) fails.

This method creates an `Environment` with default settings. To open an `Environment` with custom settings, use the [EnvironmentConfig](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/env/EnvironmentConfig.java) class. For example, to open an environment with garbage collection disabled:
```java
Environment env = Environments.newInstance("/home/me/.myAppData", new EnvironmentConfig().setGcEnabled(false));
```
If you want to bind transactions to Java threads using the **Environments** class, you can also open a [ContextualEnvironment](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/env/ContextualEnvironment.java):
```java
ContextualEnvironment env = Environments.newContextualInstance("/home/me/.myAppData");
```
A contextual environment is always aware of the transaction that is started in the current thread. The `getCurrentTransaction()` method returns the instance of a transaction that is created in current thread, if it exists. The `getAndCheckCurrentTransaction()` method returns a not-null instance of the current transaction or throws `ExodusException` if there is no transaction.

When you are finished working with the `Environment`, you should close it:
```java
env.close();
```
Before closing the `Environment`, all transactions created against it should be finished (committed or aborted). By default, it will fail if this requirement is not met.

## Transactions

[Transaction](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/env/Transaction.java) is required for any access to data in the database. Any transaction holds a database snapshot (a version of the database), thus providing [snapshot isolation](https://en.wikipedia.org/wiki/Snapshot_isolation).

All changes made in a transaction are atomic and consistent if they are successfully flushed or committed. Along with snapshot isolation and configurable durability, transactions are fully [ACID-compliant](https://en.wikipedia.org/wiki/ACID). By default, transaction durability is turned off since it significantly slows down `Transaction.flush()` and `Transaction.commit()`. To turn it on, pass `true` to `EnvironmentConfig.setLogDurableWrite()`.

Transactions can be read-only or not. Use read-only transactions to read and not update data. Transactions can also be exclusive. Use exclusive transactions to have successive access to the database. If you have an exclusive transaction, no other transaction (except read-only) can be started against the same [Environment](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/env/Environment.java) unless you
finish (commit or abort) the exclusive transaction.

Given that you have an instance of `Environment`, you can start a new transaction:
```java
final Transaction txn = environment.beginTransaction();
```
To start a read-only transaction:
```java
final Transaction txn = environment.beginReadonlyTransaction();
```
Any attempt to modify data in a read-only transaction fails with `ReadonlyTransactionException`.

To start an exclusive transaction:
```java
final Transaction txn = environment.beginExclusiveTransaction();
```
Any transaction should be finished, meaning that it is either aborted or committed. A transaction can also be flushed or reverted. The methods `commit()` and `flush()` return `true` if they succeed. If any method returns `false`, a database version mismatch has occurred. In this case, there are two possibilities: to abort the transaction and finish or revert the transaction and continue. Reverting the transaction moves it to the latest (newest) database snapshot, and database operations can be repeated against it. Thus, we get a kind of optimistic spinning:
```java
final Transaction txn = beginTransaction();
try {
    while (true) {
        // do something 
        if (txn.flush()) {
            break;
        }
        txn.revert();
    }
} finally {
    txn.abort();
}
```
This cycle is exactly the body of the `Environment.executeInTransaction()` method. The `Environment.executeInReadonlyTransaction()` method does not spin in this manner, as it cannot result in a database version mismatch:
```java
final Transaction txn = beginReadonlyTransaction();
try {
    executable.execute(txn);
} finally {
    txn.abort();
}
```
The methods `Environment.computeInTransaction()` and `Environment.computeInReadonlyTransaction()` let you compute and return a result in a transaction.

To execute a `TransactionalExecutable` or compute a `TransactionalComputable` in an exlusive transaction, use the `Environment.executeInExclusiveTransaction()` and `Environment.computeInExclusiveTransaction()` methods.

## ByteIterables

Any key and value should be a [ByteIterable](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/ByteIterable.java). A `ByteIterable` is a mix of iterable and array. It lets you lazily enumerate bytes without boxing. On the other hand, you can get its length using the `getLength()` method. Generally, iterating over bytes of `ByteIterable` is performed by means of getting [ByteIterator](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/ByteIterator.java). Multiple `ByteIterable`s are comparable. The order of a `ByteIterable` is defined like [this](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/util/ByteIterableUtil.java#L26):
```java
public static int compare(@NotNull final ByteIterable key1, @NotNull final ByteIterable key2) {
    return compare(key1.getBytesUnsafe(), key1.getLength(), key2.getBytesUnsafe(), key2.getLength());
}

public static int compare(@NotNull final byte[] key1, final int len1, @NotNull final byte[] key2, final int len2) {
    final int min = Math.min(len1, len2);
    
    for (int i = 0; i < min; i++) {
        final byte b1 = key1[i];
        final byte b2 = key2[i];
        if (b1 != b2) {
            return (b1 & 0xff) - (b2 & 0xff);
        }
    }

    return (len1 - len2);
}
```  
There are several built-in byte iterables:<br>
- [ArrayByteIterable](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/ArrayByteIterable.java) is useful to create a `ByteIterable` from a byte array or its part.
- [ByteBufferByteIterable](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/ByteBufferByteIterable.java) is (similarly to `ArrayByteIterable`) an adapter to `java.nio.ByteBuffer`.
- [FileByteIterable](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/FileByteIterable.java) can be used to iterate over a region of a file.   
- [FixedLengthByteIterable](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/FixedLengthByteIterable.java) helps to create a new `ByteIterable` as a part of another (source) `ByteIterable`.
- [CompoundByteIterable](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/CompoundByteIterable.java) can be used to create a compound byte iterable that is composed of several sub-iterables.

## Bindings

Bindings are used to represent comparable Java objects as a `ByteIterable`. There are several inheritors of the [ComparableBinding](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/bindings/ComparableBinding.java) class that let you serialize the value of any Java primitive type or `java.lang.String` to a `ByteIterable`, as well as deserialize a `ByteIterable` to a value. Bindings save the order of values. This means that the greater the value, the greater the `ByteIterable` entry. All inheritors of the `ComparableBinding` class contain two static methods: one for getting the `ByteIterable` entry from a value, and another for getting value from an entry. For example, [ByteBinding](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/bindings/ByteBinding.java) contains the following methods:
```java
public static byte entryToByte(@NotNull final ByteIterable entry);

public static ArrayByteIterable byteToEntry(final byte object);
```
[StringBinding](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/bindings/StringBinding.java) serializes `java.lang.String` objects to UTF-8 zero-terminated entries.
 
[IntegerBinding](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/bindings/IntegerBinding.java) and [LongBinding](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/bindings/LongBinding.java) serialize values to entries of lengths of 4 and 8, respectively. In addition, these classes have a pair of methods for serialization/deserialization of non-negative values to/from compressed entries:
```java
public static ArrayByteIterable intToCompressedEntry(final int object);

public static int compressedEntryToInt(@NotNull final ByteIterable entry);

public static ArrayByteIterable longToCompressedEntry(final long object);

public static long compressedEntryToLong(@NotNull final ByteIterable bi);
```
The lower the value, the shorter the compressed entry. In some cases, compressed entries let you significantly decrease database size. Serialization of non-negative integers and longs to compressed entries also saves the order of values.

## Stores

To open a store, you need an instance of `Environment` and an open transaction. Stores can be opened with and without duplicate keys, with and without key prefixing. This is controlled by the `StoreConfig` enum. For example, with a store name `"MyStore"` and `txn` as an instance of `Transaction`:
```java
// opening a store without key duplicates and without prefixing
Store store = env.openStore("MyStore", StoreConfig.WITHOUT_DUPLICATES, txn);
// opening a store with key duplicates and without prefixing
store = env.openStore("MyStore", StoreConfig.WITH_DUPLICATES, txn);
// opening a store without key duplicates and with prefixing
store = env.openStore("MyStore", StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, txn);
// opening a store with key duplicates and with prefixing
store = env.openStore("MyStore", StoreConfig.WITH_DUPLICATES_WITH_PREFIXING, txn);
```
If the `"MyName"` store doesn't exist, it is created after the transaction is flushed or committed. If it is known that a store definitely exists, it can be opened with the `StoreConfig.USE_EXISTING` configuration. In this case, you don't need to know whether the store can have duplicate keys or was created with key prefixing.

Stores with and without key prefixing are implemented by different types of search trees. [Patricia trie](http://en.wikipedia.org/wiki/Radix_tree) is the type of search tree for stores with key prefixing, and a kind of [B+tree](http://en.wikipedia.org/wiki/B%2B_tree) - for stores without key prefixing. These differ in performance characteristics: stores with key prefixing have better random key access, whereas stores without key prefixing are preferable for sequential access in order of keys.

Stores are rather stateless objects, so they can be shared in multi-threaded environments. The only exceptions are the `Environment.truncateStore()`, `Environment.removeStore()`, and `Environment.clear()` methods. After truncating, any store should be re-opened, after removing it just cannot be used. Clearing the environment makes it empty, all the data will be lost. You can open a store for each database operation, but it results in some performance overhead. The `Store.close()` method is deprecated and has no effect. 

#### Writing 

You can use the following methods to write key/value pairs:<br>
- [put()](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/env/Store.java#L94)<br>
  Puts a key/value pair into the store and returns the result. For stores with key duplicates, it returns `true` if the pair didn't exist in the store. For stores without key duplicates, it returns `true` if the key didn't exist or the new value differs from the existing one.
- [putRight()](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/env/Store.java#L106)<br>
  This method is very similar to the `put()` method. It can be used if it is certain that the key is definitely greater than any other key in the store. In that case, no search is been done before insertion, so `putRight()` can perform several times faster than `put()`. It can be useful for auto-generated keys.
- [add()](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/env/Store.java#L122)<br>
  Adds a key/value pair to the store if the key doesn't exist. For stores with and without key duplicates, it returns `true` if and only if the key doesn't exist. It never overwrites the value of an existing key.

#### Reading

Getting a value by a key can be done using the `get()` method. For stores without key duplicates, it returns a not-null value or null if the key doesn't exist. For stores with key duplicates, it returns the smallest not-null value associated with the key or null if no key exists. To check if a key/value pair exists in the store, use the `exists()` method. To get the total number of key/value pairs in the store, use the `count()` method. For more complex reading access, use [Cursors](https://github.com/JetBrains/xodus/wiki/Environments#cursors), which have a richer API and are better to use.    

#### Deleting

Use the [delete()](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/env/Store.java#L133) method to delete a key/value pair by a key. For stores without key duplicates, it deletes a single key/value pair and returns `true` if a pair was deleted. For stores with key duplicates, it deletes all pairs with the given key and returns `true` if any were deleted. To implement complex deletion cases, use [Cursors](https://github.com/JetBrains/xodus/wiki/Environments#cursors).  

#### ContextualStore

ContextualStore is a `Store` created by a `ContextualEnvironment`. Just like `ContextualEnvironment`, it is aware of the [transaction]() that is started in the current thread. `ContextualStore` overloads all `Store`'s methods with the methods that don't accept a transaction instance.
```java
final ContextualEnvironment env = Environments.newContextualInstance("/home/me/.myAppData");
final ContextualStore store = env.openStore("MyStore", StoreConfig.WITHOUT_DUPLICATES);
final ByteIterable value = store.get(stringToEntry("myKey"));
env.close();
```
In this snippet, the `get()` method doesn't accept the transaction and instead calls `ContextualEnvironment.getAndCheckCurrentTransaction()` to get a transaction instance that is associated with current thread.

## Cursors

[Cursors](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/env/Cursor.java) allow access to key/value pairs in both successive and random order. Cursor can be opened for a `Store` in a transaction. Finally, any cursor should always be closed. The cursor interface is `AutoCloseable`, so it's easy to use cursors with the try-with-resources statement.

#### Traversing key/value Pairs

```java
try (Cursor cursor = store.openCursor(txn)) {
    while (cursor.getNext()) {
        cursor.getKey();   // current key
        cursor.getValue(); // current value
    }
}
```
This code sample demonstrates traversing all key/value pairs of given store in ascending order. First, the `getNext()` method sets a cursor to hold the first (leftmost) pair. Subsequent calls to `getNext()` move the cursor to the right. Use the `getKey()` and `getValue()` methods to get data of the key/value pair that is currently held by the cursor. To traverse all of the pairs in descending order, replace `getNext()` with the `getPrev()` method:
```java
try (Cursor cursor = store.openCursor(txn)) {
    while (cursor.getPrev()) {
        // ...
    }
}
```
In these samples, all the keys are unique if the store has no duplicate keys. For stores with duplicate keys, it is also possible to traverse unique keys:
```java
try (Cursor cursor = store.openCursor(txn)) {
    while (cursor.getNextNoDup()) {
        cursor.count(); // count() returns the number of key/value pairs with current key.
    }
}
```
The `getNextNoDup()` method moves the cursor to the next key/value pair with a different key. The `count()` method returns the number of key/value pairs with the current key. For stores without duplicate keys, it always returns 1.

#### Navigating to Keys and Values

For stores with duplicate keys, you can search for a given key and traverse all key/value pairs with the same key:
```java
try (Cursor cursor = store.openCursor(txn)) {
    final ByteIterable v = cursor.getSearchKey(key);
    if (v != null) {
        // there is a value for specified key, the variable v contains the leftmost value
        while (cursor.getNextDup()) {
            // this loop traverses all pairs with the same key, values differ on each iteration
        }
    }    
}
```
For stores with duplicate keys, you can search for a given key and value:
```java
try (Cursor cursor = store.openCursor(txn)) {
    if (cursor.getSearchBoth(key, value)) {
        // cursor moved to specified pair
    }    
}
```

#### Deleting key/value Pairs

For stores with duplicate keys, you can't delete given key/value pair with `Store` methods. The `Cursor` method lets you navigate to a specified key/value pair and delete it: 
```java
try (Cursor cursor = store.openCursor(txn)) {
    if (cursor.getSearchBoth(key, value)) {
        cursor.deleteCurrent();
    }    
}
```

#### Range Search

Cursors let you navigate to the nearest key or key/value pair which is equal to or greater than specified one. For this, use the following methods:
- [getSearchKeyRange()](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/env/Cursor.java#L191)<br>
  Moves the cursor to the first pair in the store whose key is equal to or greater than the specified key. It returns a not-null value if it succeeds or null if nothing is found.  
- [getSearchBothRange()](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/env/Cursor.java#L222)<br>
  Moves the cursor to the first pair in the store whose key matches the specified key and whose value is equal to or greater than the specified value. If the store supports duplicate keys, then on matching the key, the cursor is moved to the duplicate pair with the smallest value that is equal to or greater than the specified value. Like the `getSearchKeyRange()` method, it returns not-null value if it succeeds or null if nothing is found.