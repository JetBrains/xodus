# SQL Methods

SQL Methods are similar to [SQL functions](SQL-Functions.md) but they apply to values. 
In Object Oriented paradigm they are called "methods", as functions related to a class. So what's the difference between a function and a method?

This is a [SQL function](SQL-Functions.md):
```sql
SELECT sum( salary ) FROM employee
```

This is a SQL method:
```sql
SELECT FROM salary.toJSON() FROM employee
```

As you can see the method is executed against a property/value. Methods can receive parameters, like functions.
You can concatenate N operators in sequence.

>**Note**: operators are case-insensitive.

## Bundled methods

### Methods by category
| Conversions | String manipulation | Collections | Misc |
|-------|-------|-------|-------|
|[convert()](SQL-Methods.md#convert)|[append()](SQL-Methods.md#append)|[\[\]](SQL-Methods.md)|[exclude()](SQL-Methods.md#exclude)|
|[asBoolean()](SQL-Methods.md#asboolean)|[charAt()](SQL-Methods.md#charat)|[size()](SQL-Methods.md#size)|[include()](SQL-Methods.md#include)|
|[asDate()](SQL-Methods.md#asdate)|[indexOf()](SQL-Methods.md#indexof)|[remove()](SQL-Methods.md#remove)|[javaType()](SQL-Methods.md#javatype)|
|[asDatetime()](SQL-Methods.md#asdatetime)|[left()](SQL-Methods.md#left)|[removeAll()](SQL-Methods.md#removeall)|[toJSON()](SQL-Methods.md#tojson)|
|[normalize()](SQL-Methods.md#normalize)|[right()](SQL-Methods.md#right)|[keys()](SQL-Methods.md#keys)||
|[asFloat()](SQL-Methods.md#asfloat)|[prefix()](SQL-Methods.md#prefix)|[values()](SQL-Methods.md#values)|
|[asInteger()](SQL-Methods.md#asinteger)|[trim()](SQL-Methods.md#trim)|
|[asList()](SQL-Methods.md#aslist)|[replace()](SQL-Methods.md#replace)|
|[asLong()](SQL-Methods.md#aslong)|[length()](SQL-Methods.md#length)|
|[asMap()](SQL-Methods.md#asmap)|[subString()](SQL-Methods.md#substring)|
|[asSet()](SQL-Methods.md#asset)|[toLowerCase()](SQL-Methods.md#tolowercase)|
|[asString()](SQL-Methods.md#asstring)|[toUpperCase()](SQL-Methods.md#touppercase)|
||[hash()](SQL-Methods.md#hash)|
||[format()](SQL-Methods.md#format)|


### Methods by name
|       |       |       |       |       |       
|-------|-------|-------|-------|-------|
|[\[\]](SQL-Methods.md)|[append()](SQL-Methods.md#append)|[asBoolean()](SQL-Methods.md#asboolean)|[asDate()](SQL-Methods.md#asdate)|[asDatetime()](SQL-Methods.md#asdatetime)|
|[asFloat()](SQL-Methods.md#asfloat)|[asInteger()](SQL-Methods.md#asinteger)|[asList()](SQL-Methods.md#aslist)|[asLong()](SQL-Methods.md#aslong)|[asMap()](SQL-Methods.md#asmap)|
|[asSet()](SQL-Methods.md#asset)|[asString()](SQL-Methods.md#asstring)|[charAt()](SQL-Methods.md#charat)|[convert()](SQL-Methods.md#convert)|[exclude()](SQL-Methods.md#exclude)|[format()](SQL-Methods.md#format)|
|[hash()](SQL-Methods.md#hash)|[include()](SQL-Methods.md#include)|[indexOf()](SQL-Methods.md#indexof)|[javaType()](SQL-Methods.md#javatype)|[keys()](SQL-Methods.md#keys)|[left()](SQL-Methods.md#left)|
|[length()](SQL-Methods.md#length)|[normalize()](SQL-Methods.md#normalize)|[prefix()](SQL-Methods.md#prefix)|[remove()](SQL-Methods.md#remove)|[removeAll()](SQL-Methods.md#removeall)|[replace()](SQL-Methods.md#replace)|
|[right()](SQL-Methods.md#right)|[size()](SQL-Methods.md#size)|[subString()](SQL-Methods.md#substring)|[trim()](SQL-Methods.md#trim)|[toJSON()](SQL-Methods.md#tojson)|[toLowerCase()](SQL-Methods.md#tolowercase)|
|[toUpperCase()](SQL-Methods.md#touppercase)|[values()](SQL-Methods.md#values)|

### `[]`
Execute an expression against the item. 
An item can be a multi-value object like a map, a list, an array or a entity. For entities and maps, the item must be a string. 
For lists and arrays, the index is a number.

Syntax: ```<value>[<expression>]```

Applies to the following types:
- entity,
- map,
- list,
- array

#### Examples

Get the item with key "phone" in a map:
```sql
SELECT FROM Profile WHERE '+39' IN contacts[phone].left(3)
```

Get the first 10 tags of posts:
```sql
SELECT FROM tags[0-9] FROM Posts
```

---

### .append()
Appends a string to another one.

Syntax: ```<value>.append(<value>)```

Applies to the following types:
- string

#### Examples

```sql
SELECT name.append(' ').append(surname) FROM Employee
```

---

### .asBoolean()
Transforms the property into a Boolean type. 
If the origin type is a string, then "true" and "false" is checked. If it's a number then 1 means TRUE while 0 means FALSE.

Syntax: ```<value>.asBoolean()```

Applies to the following types:
- string,
- short,
- int,
- long

#### Examples

```sql
SELECT FROM Users WHERE online.asBoolean() = true
```

---

### .asDate()
Transforms the property into a Date type.

Syntax: ```<value>.asDate()```

Applies to the following types:
- string,
- long

#### Examples

Time is stored as long type measuring milliseconds since a particular day. Returns all the records where time is before the year 2010:

```sql
SELECT FROM Log WHERE time.asDateTime() < '01-01-2010 00:00:00' 
```

---

### .asDateTime()
Transforms the property into a Date type but parsing also the time information.

Syntax: ```<value>.asDateTime()```

Applies to the following types:
- string,
- long

#### Examples

Time is stored as long type measuring milliseconds since a particular day. Returns all the records where time is before the year 2010:

```sql
SELECT FROM Log WHERE time.asDateTime() < '01-01-2010 00:00:00' 
```

---

### .asFloat()
Transforms the property into a float type.

Syntax: ```<value>.asFloat()```

Applies to the following types:
- any

#### Examples

```sql
SELECT ray.asFloat() > 3.14
```
---

### .asInteger()
Transforms the property into an integer type.

Syntax: ```<value>.asInteger()```

Applies to the following types:
- any

#### Examples

Converts the first 3 chars of 'value' field in an integer:
```sql
SELECT value.left(3).asInteger() FROM Log
```

---

### .asList()
Transforms the value in a List. If it's a single item, a new list is created.

Syntax: ```<value>.asList()```

Applies to the following types:
- any

#### Examples

```sql
SELECT tags.asList() FROM Friend
```

---

### .asLong()
Transforms the property into a Long type.

Syntax: ```<value>.asLong()```

Applies to the following types:
- any

#### Examples

```sql
SELECT date.asLong() FROM Log
```
---

### .asMap()
Transforms the value in a Map where even items are the keys and odd items are values.

Syntax: ```<value>.asMap()```

Applies to the following types:
- collections

#### Examples

```sql
SELECT tags.asMap() FROM Friend
```

---

### .asSet()
Transforms the value in a Set. If it's a single item, a new set is created. Sets doesn't allow duplicates.

Syntax: ```<value>.asSet()```

Applies to the following types:
- any

#### Examples

```sql
SELECT tags.asSet() FROM Friend
```

---

### .asString()
Transforms the field into a string type.

Syntax: ```<value>.asString()```

Applies to the following types:
- any

#### Examples

Get all the salaries with decimals:
```sql
SELECT FROM Profie WHERE salary.asString().indexof('.') > -1
```

---

### .charAt()
Returns the character of the string contained in the position 'position'. 'position' starts from 0 to string length.

Syntax: ```<value>.charAt(<position>)```

Applies to the following types:
- string

#### Examples

Get the first character of the users' name:
```sql
SELECT FROM User WHERE name.charAt( 0 ) = 'L'
```

---

### .convert()
Convert a value to another type.

Syntax: ```<value>.convert(<type>)```

Applies to the following types:
- any

#### Examples

```sql
SELECT dob.convert( 'date' ) FROM User
```

---

### .exclude()
Excludes some properties in the resulting record.

Syntax: ```<value>.exclude(<field-name>[,]*)```

Applies to the following types:
- entity

#### Examples

```sql
SELECT EXPAND( @this.exclude( 'password' ) ) FROM User
```


You can specify a wildcard as ending character to exclude all the properties names of which that start with a certain string.
Example:

```sql
SELECT EXPAND( @this.exclude( 'out_*', 'in_*' ) ) FROM Entity
```

---

### .format()
Returns the value formatted using the common "printf" syntax. 
For the complete reference goto [Java Formatter JavaDoc](http://java.sun.com/j2se/1.5.0/docs/api/java/util/Formatter.html#syntax). 

Syntax: ```<value>.format(<format>)```

Applies to the following types:
- any

#### Examples
Formats salaries as number with 11 digits filling with 0 at left:

```sql
SELECT salary.format("%-011d") FROM Employee
```

---

### .hash()

Returns the hash of the property. Supports all the algorithms available in the JVM.

Syntax: ```<value>.hash([<algorithm>])```

Applies to the following types:
- string

#### Example

Get the SHA-512 of the property "password" in the class User:

```sql
SELECT password.hash('SHA-512') FROM User
```

---

### .include()
Include only some properties in the resulting record.

Syntax: ```<value>.include(<property-name>[,]*)```

Applies to the following types:
- entity

#### Examples

```sql
SELECT EXPAND( @this.include( 'name' ) ) FROM User
```
You can specify a wildcard as ending character to inclide all the fields that start with a certain string. 
  Example to include all the fields that starts with `amonut`:

```sql
SELECT EXPAND( @this.include( 'amount*' ) ) FROM V
```

---

### .indexOf()
Returns the position of the 'string-to-search' inside the value. It returns -1 if no occurrences are found. 
  'begin-position' is the optional position where to start, otherwise the beginning of the string is taken (=0).

Syntax: ```<value>.indexOf(<string-to-search> [, <begin-position>)```

Applies to the following types:
- string

#### Examples
Returns all the UK numbers:
```sql
SELECT FROM Contact WHERE phone.indexOf('+44') > -1
```

---

### .javaType()
Returns the corresponding Java Type.

Syntax: ```<value>.javaType()```

Applies to the following types:
- any

#### Examples
Prints the Java type used to store dates:
```sql
SELECT FROM date.javaType() FROM Events
```

---

### .keys()
Returns the map's keys as a separate set. Useful to use in conjunction with IN, CONTAINS and CONTAINSALL operators.

Syntax: ```<value>.keys()```

Applies to the following types:
- maps
- entities

#### Examples
```sql
SELECT FROM Actor WHERE 'Luke' IN map.keys()
```

---

### .left()
Returns a substring of the original cutting from the begin and getting 'len' characters.

Syntax: ```<value>.left(<length>)```

Applies to the following types:
- string

#### Examples
```sql
SELECT FROM Actors WHERE name.left( 4 ) = 'Luke'
```

---

### .length()
Returns the length of the string. If the string is null 0 will be returned.

Syntax: ```<value>.length()```

Applies to the following types:
- string

#### Examples
```sql
SELECT FROM Providers WHERE name.length() > 0
```

---

### .normalize()
Converts string into one of unicode normaliation forms.
Form can be NDF, NFD, NFKC, NFKD. Default is NDF. pattern-matching if not defined is "\\p{InCombiningDiacriticalMarks}+". 
For more information look at <a href="http://www.unicode.org/reports/tr15/tr15-23.html">Unicode Standard</a>.

Syntax: ```<value>.normalize( [<form>] [,<pattern-matching>] )```

Applies to the following types:
- string

#### Examples
```sql
SELECT FROM V WHERE name.normalize() AND name.normalize('NFD')
```
---

### .prefix()
Prefixes a string to another one.

Syntax: ```<value>.prefix('<string>')```

Applies to the following types:
- string

#### Examples
```sql
SELECT name.prefix('Mr. ') FROM Profile
```

---

### .remove()
Removes the first occurrence of the passed items.

Syntax: ```<value>.remove(<item>*)```

Applies to the following types:
- collection

#### Examples

```sql
SELECT out().in().remove( @this ) FROM Entity
```

---

### .removeAll()
Removes all the occurrences of the passed items.

Syntax: ```<value>.removeAll(<item>*)```

Applies to the following types:
- collection

#### Examples

```sql
SELECT out().in().removeAll( @this ) FROM Entity
```

---

### .replace()
Replace a string with another one.

Syntax: ```<value>.replace(<to-find>, <to-replace>)```

Applies to the following types:
- string

#### Examples

```sql
SELECT name.replace('Mr.', 'Ms.') FROM User
```

---


### .right()
Returns a substring of the original cutting from the end of the string 'length' characters.

Syntax: ```<value>.right(<length>)```

Applies to the following types:
- string

#### Examples

Returns all the entities where the name ends by "ke".
```sql
SELECT FROM Enitity WHERE name.right( 2 ) = 'ke'
```

---

### .size()
Returns the size of the collection.

Syntax: ```<value>.size()```

Applies to the following types:
- collection

#### Examples

Returns all the items in a tree with children:
```sql
SELECT FROM TreeItem WHERE children.size() > 0
```


  ---

### .subString()
Returns a substring of the original cutting from 'begin' index up to 'end' index (not included).

Syntax: ```<value>.subString(<begin> [,<end>] )```

Applies to the following types:
- string

#### Examples

Get all the items where the name begins with an "L":
```sql
SELECT name.substring( 0, 1 ) = 'L' FROM StockItems
```

Substring of `Exodus`
```sql
SELECT "Exodus".substring(1,5)
```
returns `xodu`

---

### .trim()
Returns the original string removing white spaces from the begin and the end.

Syntax: ```<value>.trim()```

Applies to the following types:
- string

#### Examples
```sql
SELECT name.trim() == 'Luke' FROM Actors
```

---

### .toJSON()
Returns entry in JSON format.

Syntax: ```<value>.toJSON()```

Applies to the following types:
- entity
  
#### Examples
```sql
insert into Test content {"attr1": "value 1", "attr2": "value 2"}

select @this.toJson() from Test
```

---

### .toLowerCase()
Returns the string in lower case.

Syntax: ```<value>.toLowerCase()```

Applies to the following types:
- string

#### Examples
```sql
SELECT name.toLowerCase() == 'luke' FROM Actors
```

---

### .toUpperCase()
Returns the string in upper case.

Syntax: ```<value>.toUpperCase()```

Applies to the following types:
- string

#### Examples
```sql
SELECT name.toUpperCase() == 'LUKE' FROM Actors
```

---


### .values()
Returns the map's values as a separate collection. Useful to use in conjunction with IN, CONTAINS and CONTAINSALL operators.

Syntax: ```<value>.values()```

Applies to the following types:
- maps
- entities


#### Examples
```sql
SELECT FROM Clients WHERE map.values() CONTAINSALL ( name is not null)
```
