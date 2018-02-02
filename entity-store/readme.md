# <img src="https://raw.githubusercontent.com/wiki/jetbrains/xodus/xodus.png" width=160>

The **Entity Stores** layer is designed to access data as [entities](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/entitystore/Entity.java) with attributes and links. Use a [transaction](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/entitystore/StoreTransaction.java) to create, modify, read and query data. Transactions are quite similar to [those](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/env/Transaction.java) on the **Environments** layer, though the **Entity Store** API is much richer in terms of querying data. The API and the implementation live in the _jetbrains.exodus.entitystore_ package.

[PersistentEntityStore](https://github.com/JetBrains/xodus/wiki/Entity-Stores#persistententitystore)
<br>[Transactions](https://github.com/JetBrains/xodus/wiki/Entity-Stores#transactions)
<br>[Entities](https://github.com/JetBrains/xodus/wiki/Entity-Stores#entities)
<br>[Properties](https://github.com/JetBrains/xodus/wiki/Entity-Stores#properties)
<br>[Links](https://github.com/JetBrains/xodus/wiki/Entity-Stores#links)
<br>[Blobs](https://github.com/JetBrains/xodus/wiki/Entity-Stores#blobs)
<br>[Queries](https://github.com/JetBrains/xodus/wiki/Entity-Stores#queries)
<br>&nbsp;&nbsp;&nbsp;&nbsp;[Iterating over All Entities of Specified Type](https://github.com/JetBrains/xodus/wiki/Entity-Stores#iterating-over-all-entities-of-specified-type)
<br>&nbsp;&nbsp;&nbsp;&nbsp;[EntityIterable](https://github.com/JetBrains/xodus/wiki/Entity-Stores#entityiterable)
<br>&nbsp;&nbsp;&nbsp;&nbsp;[EntityIterator](https://github.com/JetBrains/xodus/wiki/Entity-Stores#entityiterator)
<br>&nbsp;&nbsp;&nbsp;&nbsp;[Searching by Property Value](https://github.com/JetBrains/xodus/wiki/Entity-Stores#searching-by-property-value)
<br>&nbsp;&nbsp;&nbsp;&nbsp;[Searching in Range of Property Values](https://github.com/JetBrains/xodus/wiki/Entity-Stores#searching-in-range-of-property-values)
<br>&nbsp;&nbsp;&nbsp;&nbsp;[Traversing Links](https://github.com/JetBrains/xodus/wiki/Entity-Stores#traversing-links)
<br>&nbsp;&nbsp;&nbsp;&nbsp;[SelectDistinct and SelectManyDistinct](https://github.com/JetBrains/xodus/wiki/Entity-Stores#selectdistinct-and-selectmanydistinct)
<br>&nbsp;&nbsp;&nbsp;&nbsp;[Binary Operations](https://github.com/JetBrains/xodus/wiki/Entity-Stores#binary-operations)
<br>&nbsp;&nbsp;&nbsp;&nbsp;[Searching for Entities Having Property, Link, Blob](https://github.com/JetBrains/xodus/wiki/Entity-Stores#searching-for-entities-having-property-link-blob)
<br>&nbsp;&nbsp;&nbsp;&nbsp;[Sorting](https://github.com/JetBrains/xodus/wiki/Entity-Stores#sorting)
<br>&nbsp;&nbsp;&nbsp;&nbsp;[Other Goodies](https://github.com/JetBrains/xodus/wiki/Entity-Stores#other-goodies)
<br>[Sequences](https://github.com/JetBrains/xodus/wiki/Entity-Stores#sequences)

## PersistentEntityStore

To open or create an entity store, create an instance of [PersistentEntityStore](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/entitystore/PersistentEntityStore.java) with the help of the [PersistentEntityStores](https://github.com/JetBrains/xodus/blob/master/entity-store/src/main/java/jetbrains/exodus/entitystore/PersistentEntityStores.java) utility class:
```java
PersistentEntityStore entityStore = PersistentEntityStores.newInstance("/home/me/.myAppData");
```
`PersistentEntityStore` works over `Environment`, so the method that is shown above implicitly creates an `Environment` with the same location. Each `PersistentEntityStore` has a name. You can create several entity stores with different names over an `Environment`. If you don't specify a name, the [default name](https://github.com/JetBrains/xodus/blob/master/entity-store/src/main/java/jetbrains/exodus/entitystore/PersistentEntityStores.java#L30) is used. 

`PersistentEntityStore` has different methods to create an instance of a `PersistentEntityStore`. In addition to the underlying `Environment`, you can specify the [BlobValut](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/entitystore/BlobVault.java) and [PersistentEntityStoreConfig](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/entitystore/PersistentEntityStoreConfig.java). `BlobVault` is a base class that describes an interface to binary large objects (BLOBs) that are used internally by the implementation of a `PersistentEntityStore`. If you don't specify a `BlobVault` when you create a `PersistentEntityStore`, an instance of the [FileSystemBlobVault](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/entitystore/FileSystemBlobVault.java) class is used. If you don't specify the `PersistentEntityStoreConfig` when you create a `PersistentEntityStore`, [PersistentEntityStoreConfig.DEFAULT](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/entitystore/PersistentEntityStoreConfig.java#L58) is used.

Like [ContextualEnvironment](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/env/ContextualEnvironment.java), `PersistentEntityStore` is always aware of the transaction that is started in the current thread. The [getCurrentTransaction()](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/entitystore/EntityStore.java#L84) method returns the transaction that is started in the current thread or null if there is no such transaction.

When you are finished working with a `PersistentEntityStore`, call the `close()` method.

## Transactions

[Entity store transactions](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/entitystore/StoreTransaction.java) are quite similar to the `Environment` layer transactions. To manually start a transaction, use [beginTransaction()](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/entitystore/EntityStore.java#L56):
```java
final StoreTransaction txn = store.beginTransaction(); 
```
or [beginReadonlyTransaction()](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/entitystore/EntityStore.java#L67):
```java
final StoreTransaction txn = store.beginReadonlyTransaction();
```
An attempt to modify data in a read-only transaction fails with a `ReadonlyTransactionException`.
 
Any transaction should be finished, meaning that it is either aborted or committed. The transaction can also be flushed or reverted. The methods `commit()` and `flush()` return `true` if they succeed. If any method returns `false`, a database version mismatch has occurred. In this case, there are two possibilities: to abort the transaction and finish or revert the transaction and continue. An unsuccessful flush implicitly reverts the transaction and moves it to the latest (newest) database snapshot, so database operations can be repeated against it:
```java
StoreTransaction txn = beginTransaction();
try {
    do {
        // do something
        // if txn has already been aborted in user code
        if (txn != getCurrentTransaction()) {
            txn = null;
            break;
        }
    } while (!txn.flush());
} finally {
    // if txn has not already been aborted in execute()
    if (txn != null) {
        txn.abort();
    }
}
```
If you don't care for such spinning and don't want to control the results of `flush()` and `commit()`, you can use the
[executeInTransaction()](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/entitystore/PersistentEntityStore.java#L63),
[executeInExclusiveTransaction()](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/entitystore/PersistentEntityStore.java#L73),
[executeInReadonlyTransaction()](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/entitystore/PersistentEntityStore.java#L84),
[computeInTransaction()](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/entitystore/PersistentEntityStore.java#L94) and
[computeInReadonlyTransaction()](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/entitystore/PersistentEntityStore.java#L104) methods. 

## Entities

**Entities** can have properties and blobs, and can be linked. Each property, blob, or link is identified by its name. Although entity properties are expected to be `Comparable`, only Java primitive types, Strings, and [ComparableSet](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/bindings/ComparableSet.java) values can be used by default. Use the [PersistentEntityStore.registerCustomPropertyType()](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/entitystore/PersistentEntityStore.java#L136) method to define your own property type.

Imagine that your application must include a user management system. All further samples imply that you have accessible `StoreTransaction txn`. Let's create a new user:
```java
final Entity user = txn.newEntity("User");
```
Each [Entity](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/entitystore/Entity.java) has a string entity type and its unique ID which is described by [EntityId](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/entitystore/EntityId.java): 
```java
final String type = user.getType();
final EntityId id = user.getId();
```
The entity ID may be used as a part of URL or in any other way to load the entity:
```java
final Entity user = txn.getEntity(id);
```

## Properties
 
Let's create a user with a specific `loginName`, `fullName`, `email` and `password`:
```java
final Entity user = txn.newEntity("User");
user.setProperty("login", loginName);
user.setProperty("fullName", fullName);
user.setProperty("email", email);
final String salt = MessageDigestUtil.sha256(Double.valueOf(Math.random()).toString());
user.setProperty("salt", salt);
user.setProperty("password", MessageDigestUtil.sha256(salt + password));
```
The [MessageDigestUtil](https://github.com/JetBrains/xodus/blob/master/utils/src/main/java/jetbrains/exodus/core/crypto/MessageDigestUtil.java) class from the `utils` module is used to encrypt the password.

## Links

The user management system should probably be able to save additional information about a user, including age, bio, and avatar. It's reasonable not to save this information directly in a _User_ entity, but to create a _UserProfile_ one and link it with the user:
```java
final Entity userProfile = txn.newEntity("UserProfile");
userProfile.setLink("user", user);
user.setLink("userProfile", userProfile);
userProfile.setProperty("age", age);
```
Reading profile of a user:
```java
final Entity userProfile = user.getLink("userProfile");
if (userProfile != null) {
    // read properties of userProfile
}
```
The method `setLink()` sets the new link and overrides previous one. It is also possible to add a new link that does not affect existing links. Suppose users can be logged in with the help of different _AuthModules_, such as LDAP or OpenID. It makes sense to create an entity for each auth module and link it with the user:
```java
final Entity authModule = txn.newEntity("AuthModule");
authModule.setProperty("type", "LDAP");
user.addLink("authModule", authModule);
authModule.setLink("user", user);
```
Iterating over all user's auth modules:
```java
for (Entity authModule: user.getLinks("authModule")) {
    // read properties of authModule
}
```
It's also possible to delete a specific auth module:
```java
user.deleteLink("authModule", authModule);
```
or delete all available auth modules:
```java
user.deleteLinks("authModule");
```
 
## Blobs

Some properties cannot be expressed as Strings or primitive types, or their values are too large. For these cases, it is better to save large strings (like the biography of a user) in a blob string instead of a property. For raw binary data like images and media, use blobs:
```java
userProfile.setBlobString("bio", bio);
userProfile.setBlob("avatar", file);
```
A blob string is similar to a property, but it cannot be used in [Search Queries](https://github.com/JetBrains/xodus/wiki/Entity-Stores#search-queries). To read a blob string, use the `Entity.getBlobString()` method.

The value of a blob can be set as `java.io.InputStream` or `java.io.File`. The second method is preferred when setting a blob from a file. To read a blob, use the `Entity.getBlob()` method. You are not required to and should not close the input stream that is returned by the method. Concurrent access to a single blob within a single transaction is not possible.  

## Queries

`StoreTransaction` contains a lot of methods to query, sort, and filter entities. All of them return an instance of [EntityIterable](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/entitystore/EntityIterable.java).

#### Iterating over All Entities of Specified Type

Let's iterate over all users and print their full names:
```java
final EntityIterable allUsers = txn.getAll("User);
for (Entity user: allUsers) {
    System.out.println(user.getProperty("fullName"));
}
```
As you can see, `EntityIterable` is `Iterable<Entity>`.

#### EntityIterable

`EntityIterate` lets you lazily iterate over entities. `EntityIterable` is valid only against a particular database snapshot, so finishing the transaction or moving it to the newest snapshot (`flush()`, `revert()`) breaks the iteration. If you need to flush the current transaction during an iteration over an `EntityIterable`, you have to manually load the entire entity iterable to a list and then iterate over the list. 

You can find out the size of `EntityIterable` without iterating:
```java
final long userCount = txn.getAll("User).size();
```

Even though the `size()` method performs faster than an iteration, it can be quite slow for some iterables. Xodus does a lot of caching internally, so sometimes the size of an `EntityIterable` can be computed quite quickly. You can check if it can be computed quickly by using the `count()` method:
```java
final long userCount = txn.getAll("User").count();
if (userCount >= 0) {
    // result for txn.getAll("User") is cached, so user count is known
}
```

The `count()` method checks if the result (a sequence of entity ids) is cached for the `EntityIterable`. If the sequence is cached, the size is returned quickly. If the result is not cached, the `count()` method returns `-1`.

In addition to `size()` and `count()`, which always return an actual value (if not `-1`), there are _eventually consistent_ methods `getRoughCount()` and `getRoughSize()`. If the result for the `EntityIterable` is cached, these methods return the same value as `count()` and `size()` do. If the result is not cached, Xodus can internally cache the value of last known size of the `EntityIterable`. If the last known size is cached, `getRoughCount()` and `getRoughSize()` return it. Otherwise, `getRoughCount()` returns `-1` and `getRoughSize()` returns the value of `size()`.

Use the `isEmpty()` method to check if an `EntityIterable` is empty. In most cases, it is faster than getting `size()`, and is returned immediately if the EntityIterable's result is cached.

#### EntityIterator

[EntityIterator](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/entitystore/EntityIterator.java) is an iterator of `EntityIterable`. It is an `Iterator<Entity>`, but it also lets you enumerate entity IDs instead of entities using method `nextId()`. Getting only IDs provides better iteration performance.

`EntityIterator` contains the `dispose()` method that releases all resources that the iterator possibly consumes. The `shouldBeDisposed()` method definitely says if it does. You can call the `dispose()` method implicitly in two cases: if an iteration finishes and `hasNext()` returns `false` and if the transaction finishes or moves to the latest snapshot (any of `commit()`, `abort()`, `flush()` or `revert()` is called). Sometimes, it makes sense to call `dispose()` manually. For example, to check whether an `EntityIterable` is empty can look like this:
```java
boolean isEmptyIterable(final EntityIterable iterable) {
    final EntityIterator it = iterable.iterator();
    final boolean result = !it.hasNext();
    if (!result && it.shouldBeDisposed()) {
        it.dispose();
    }
    return result;
}
```

#### Searching by Property Value

To log in a user with the provided credentials (`loginName` and `password`), you must first find all of the users with the specified `loginName`:
```java
final EntityIterable candidates = txn.find("User", "login", loginName);
```
Then, you have to iterate over the candidates and check if the password matches:
```java
Entity loggedInUser = null;
for (Entity candidate: candidates) {
    final String salt = candidate.getProperty("salt");
    if (MessageDigestUtil.sha256(salt + password).equals(candidate.getProperty("password"))) {
        loggedInUser = candidate;
        break;
    }
}

return loggedInUser; 
```
If you want to log in users with `email` also, calculate candidates as follows:
```java
final EntityIterable candidates = txn.find("User", "login", loginName).union(txn.find("User", "email", email));
```
To find user profiles of users with a specified age:
```java
final EntityIterable little15Profiles = txn.find("UserProfile", "age", 15);
```
Please note that search by string property values is _case-insensitive_.

#### Searching in Range of Property Values

To search for user profiles of users whose age is in the range of [17-23], inclusively:
```java
final EntityIterable studentProfiles = txn.find("UserProfile", "age", 17, 23);
```

Another case of range search is to search for entities with a string property that starts with a specific value:
```java
final EntityIterable userWithFullNameStartingWith_a = txn.findStartingWith("User", "fullName", "a");
```
Please note that search by string property values is _case-insensitive_.

#### Traversing Links

One method for traversing links is already mentioned above: [Entity.getLinks()](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/entitystore/Entity.java#L359). It is considered as a query because it returns an `EntityIterable`. It lets you iterate over outgoing links of an entity with a specified name.
 
It is also possible to find incoming links. For example, let's search for user who uses a particular auth module:
```java
final EntityIterable ldapUsers = txn.findLinks("User", ldapAuthModule, "authModule");
final EntityIterator ldapUsersIt = ldapUsers.iterator();
return ldapUsersIt.hasNext() ? ldapUsersIt.next() : null; 
```

#### SelectDistinct and SelectManyDistinct

To search for users whose age is in the range of [17-23], inclusively:
```java
final EntityIterable studentProfiles = txn.find("UserProfile", "age", 17, 23);
final EntityIterable students = studentProfiles.selectDistinct("user");
```
To get all auth modules of users whose age is in the range of [17-23], inclusively:
```java
final EntityIterable studentProfiles = txn.find("UserProfile", "age", 17, 23);
final EntityIterable students = studentProfiles.selectDistinct("user");
final EntityIterable studentAuthModules = students.selectManyDistinct("authModule");
```
Use the `selectDistinct` operation if the corresponding link is single, meaning that it is set using the `setLink()` method. If the link is multiple, meaning thatit is set using the `addLink()` method, use `selectManyDistinct`. Results of both the `selectDistinct` and `selectManyDistinct` operations never contain duplicate entities. In addition, the result of `selectManyDistinct` can contain `null`. For example, if there is a user with no auth module.
    
#### Binary operations

There are four binary operations that are defined for `EntityIterable`: _intersect()_, _union()_, _minus()_ and _concat()_. For all of them, the instance is a left operand, and the parameter is a right operand.

Let's search for users whose `login` _and_ `fullName` start with "xodus" (case-insensitively):
```java
final EntityIterable xodusUsers = txn.findStartingWith("User", "login", "xodus").intersect(txn.findStartingWith("User", "fullName", "xodus"));
```
Users whose `login` _or_ `fullName` start with "xodus":
```java
final EntityIterable xodusUsers = txn.findStartingWith("User", "login", "xodus").union(txn.findStartingWith("User", "fullName", "xodus"));
```
Users whose `login` _and not_ `fullName` start with "xodus":
```java
final EntityIterable xodusUsers = txn.findStartingWith("User", "login", "xodus").minus(txn.findStartingWith("User", "fullName", "xodus"));
```
There is no suitable sample for the `concat()` operation, it just concatenates results of two entity iterables.

The result of a binary operation (`EntityIterable`) itself can be an operand of a binary operation. You can use these results to construct a query tree of an arbitrary height.

#### Searching for Entities Having Property, Link, Blob

The `StoreTransaction.findWithProp` method returns entities of a specified type that have a property with the specified name. There are also methods `StoreTransaction.findWithBlob` and `StoreTransaction.findWithLink`.

For example, if we do not require a user to enter a full name, the `fullName` property can be null. You can get users with or without full name by using `findWithProp`:    
```java
final EntityIterable usersWithFullName = txn.findWithProp("User", "fullName");
final EntityIterable usersWithoutFullName = txn.getAll("User").minus(txn.findWithProp("User", "fullName"));
``` 
To get user profiles with avatars using `findWithBlob`:
```java
final EntityIterable userProfilesWithAvatar = txn.findWithBlob("UserProfile", "avatar");
```
The `findWithBlob` method is also applicable to blob strings:
```java
final EntityIterable userProfilesWithBio = txn.findWithBlob("UserProfile", "bio");
```
To get users with auth modules:
```java
final EntityIterable usersWithAuthModules = txn.findWithLink("User", "authModule");
```

#### Sorting

To sort all users by `login` property:
```java
final EntityIterable sortedUsersAscending = txn.sort("User", "login", true);
final EntityIterable sortedUsersDescending = txn.sort("User", "login", false);
```
To sort all users that have LDAP authentication by `login` property:
```java
// at first, find all LDAP auth modules
final EntityIterable ldapModules = txn.find("AuthModule", "type", "ldap"); // case-insensitive!
// then select users
final EntityIterable ldapUsers = ldapModules.selectDistinct("user");
// finally, sort them
final EntityIterable sortedLdapUsers = txn.sort("User", "login", ldapUsers, true);
```
Sorting can be stable. For example, to get users sorted by `login` in ascending order and by `fullName` in descending (users with the same login name are sorted by full name in descending order):
```java
final EntityIterable sortedUsers = txn.sort("User", "login", txn.sort("User", "fullName", false), true);
```
You can implement custom sorting algorithms with `EntityIterable.reverse()`. Wrap the sort results from `EntityIterable` with `EntityIterable.asSortResult()`. This lets the sorting engine recognize the sort result and use a stable sorting algorithm. If the source is not a sort result, the engine uses a non-stable sorting algorithm which is generally faster.  



## Sequences

[Sequences](https://github.com/JetBrains/xodus/blob/master/openAPI/src/main/java/jetbrains/exodus/entitystore/Sequence.java) let you  get unique successive non-negative long IDs. Sequences are named. You can request a sequence by name with the `StoreTransaction.getSequence()` method. Sequences are persistent, which means that _any_ flushed or committed transaction saves all dirty (modified) sequences which were requested by transactions created against the current `EntityStore`.