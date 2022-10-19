
# SQL - Functions

## Bundled functions

### Functions by category

|  Links  | Math  | Collections | Misc  |
|---------|-------|-------------|-------|
| [out()](SQL-Functions.md#out)    | [min()](SQL-Functions.md#min) | [set()](SQL-Functions.md#set)             | [date()](SQL-Functions.md#date)
| [in()](SQL-Functions.md#in)      | [max()](SQL-Functions.md#max) | [map()](SQL-Functions.md#map)               | [sysdate()](SQL-Functions.md#sysdate)
| [both()](SQL-Functions.md#both)  | [stddev()](SQL-Functions.md#stddev) | [list()](SQL-Functions.md#list)             | [format()](SQL-Functions.md#format)
| [traversedEntity()](SQL-Functions.md#traversedentity) | [sum()](SQL-Functions.md#sum) | [difference()](SQL-Functions.md#difference) | [distance()](SQL-Functions.md#distance)
|  | [abs()](SQL-Functions.md#abs) | [first()](SQL-Functions.md#first)           | [ifnull()](SQL-Functions.md#ifnull)
|  | [eval()](SQL-Functions.md#eval) | [intersect()](SQL-Functions.md#intersect)   | [coalesce()](SQL-Functions.md#coalesce)
|  | [avg()](SQL-Functions.md#avg) | [distinct()](SQL-Functions.md#distinct)     | [uuid()](SQL-Functions.md#uuid)|
|  | [count()](SQL-Functions.md#count) | [expand()](SQL-Functions.md#expand)|  [if()](SQL-Functions.md#if)
|  | [mode()](SQL-Functions.md#mode)                        | [unionall()](SQL-Functions.md#unionall)|  |
|  | [median()](SQL-Functions.md#median)                      | [symmetricDifference()](#symmetricdifference) |  |
|  | [percentile()](SQL-Functions.md#percentile)                  | [last()](SQL-Functions.md#last)| |
|  | [variance()](SQL-Functions.md#variance)| ||
 

SQL Functions are all the functions bundled with Xodus SQL engine. You can create your own functions in any language supported by JVM. 
Look also to [SQL Methods](SQL-Methods.md).

SQL Functions can work in 2 ways based on the fact that they can receive 1 or more parameters:

## Aggregated mode

When only one parameter is passed, the function aggregates the result in only one record. The classic example is the `sum()` function:
```sql
SELECT SUM(salary) FROM employee
```
This will always return one record: the sum of salary properties across every employee entity.

## Inline mode

When two or more parameters are passed:
```sql
SELECT SUM(salary, extra, benefits) AS total FROM employee
```
This will return the sum of the properties "salary", "extra" and "benefits" as "total". 

In case you need to use a function inline, when you only have one parameter, then add "null" as the second parameter:

```sql
SELECT first( out('friends').name, null ) as firstFriend FROM Profiles
```
In the above example, the `first()` function doesn't aggregate everything in only one record, but rather returns one record per `Profile`, where the `firstFriend` is the first item of the collection received as the parameter.

## Function Reference

### out()

Get the adjacent outgoing entities starting from the current entity.

Syntax: ```out([<property-1>][,<property-n>]*)```

Available since: 

#### Example

Get all the outgoing entitites from all the Vehicle entyties:
```sql
SELECT out() FROM Vehicle
```

Get all the outgoing entities connected by properties with names "eats" and "favorited" from all the Restaurant entities in Rome:

```sql
SELECT out('eats','favorited') FROM Restaurant WHERE city = 'Rome'
```
---
### in()

Get the adjacent incoming entities starting from the current entity.

Syntax:
```
in([<property-1>][,<property-n>]*)
```

Available since:

#### Example

Get all the incoming enities from all the Vehicle entities:

```sql
SELECT in() FROM Vehicle
```

Get all the incoming entities connected by properties with names "friend" and "brother":
```sql
SELECT in('friend','brother') FROM Profile
```
---
### both()

Get the adjacent outgoing and incoming entities starting from the current entity.

Syntax:
```
both([<property-1>][,<property-n>]*)
```

Available since: 

#### Example

Get all the incoming and outgoing entities from entity with id #13-33:

```sql
SELECT both() FROM #13-33
```

Get all the incoming and outgoing enitites connected by properties "friend" and "brother":

```sql
SELECT both('friend','brother') FROM Person
```
---

### eval()

Syntax: ```eval('<expression>')```

Evaluates the expression between quotes (or double quotes).

Available since:

#### Example

```sql
SELECT eval('price * 120 / 100 - discount') AS finalPrice FROM Order
```


### coalesce()

Returns the first property/value not null parameter. If no property/value is not null, returns null.

Syntax:
```
coalesce(<property|value> [, <property-n|value-n>]*)
```

Available since:

#### Example

```sql
SELECT coalesce(amount, amount2, amount3) FROM Account
```

### if()

Syntax:
```
if(<expression>, <result-if-true>, <result-if-false>)
```

Evaluates a condition (first parameters) and returns the second parameter if the condition is true, and the third parameter otherwise.

#### Example: 
```
SELECT if(eval("name = 'John'"), "My name is John", "My name is not John") FROM Person
```

### ifnull()

Returns the passed property/value (or optional parameter *return_value_if_not_null*). If property/value is not null, otherwise it returns *return_value_if_null*.

Syntax:
```java
ifnull(<property/value>, <return_value_if_null>)
```

Available since:

#### Example

```sql
SELECT ifnull(salary, 0) FROM Account
```

---
### expand()

Available since: 
Expands the entity pointed by that link. 

Syntax: ```expand(<property>)```


#### Example

```sql
SELECT EXPAND(addresses) FROM Account. 
```

---

### first()

Retrieves only the first item of multi-value properties (arrays, collections and maps). For non multi-value types just returns the value.

Syntax: ```first(<property>)```

Available since:

#### Example

```sql
select first(addresses) from Account
```
---
### last()

Retrieves only the last item of multi-value properties (arrays, collections and maps). For non multi-value types just returns the value.

Syntax: ```last(<property>)```

Available since:

#### Example

```sql
SELECT last( addresses ) FROM Account
```
---
### count()

Counts the entities that match the query condition. If property name is passed then entity will be counted only if the property content is not null.

Syntax: ```count(<property>)```

Available since:

#### Example

```sql
SELECT COUNT(*) FROM Account
```
---
### min()

Returns the minimum value. If invoked with more than one parameter, the function doesn't aggregate but returns the minimum value between all the arguments.

Syntax: ```min(<property-1> [, <property-n>]* )```

Available since:

#### Example

Returns the minimum salary of all the Account records:
```sql
SELECT min(salary) FROM Account
```
Returns the minimum value between 'salary1', 'salary2' and 'salary3' properties.
```sql
SELECT min(salary1, salary2, salary3) FROM Account
```
---
### max()

Returns the maximum value. If invoked with more than one parameter, the function doesn't aggregate, but returns the maximum value between all the arguments.

Syntax: ```max(<property-1> [, <property-n>]* )```

Available since:

#### Example

Returns the maximum salary of all the Account entities:
```sql
SELECT max(salary) FROM Account.
```

Returns the maximum value between 'salary1', 'salary2' and 'salary3' properties.
```sql
SELECT max(salary1, salary2, salary3) FROM Account
```

---
### abs()

Returns the absolute value. It works with Integer, Long, Short, Double, Float, null.

Syntax: ```abs(<property>)```

Available since:

#### Example

```sql
SELECT abs(score) FROM Account
SELECT abs(-2332) FROM Account
SELECT abs(999) FROM Account
```

---
### avg()

Returns the average value.

Syntax: ```avg(<property>)```

Available since: 

#### Example

```sql
SELECT avg(salary) FROM Account
```

---
### sum()

Syntax: ```sum(<property>)```

Returns the sum of all the values returned.

Available since:

#### Example

```sql
SELECT sum(salary) FROM Account
```
---
### date()

Returns a date formatting a string. &lt;date-as-string&gt; is the date in string format, and &lt;format&gt; is the date format following these [rules](http://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html). 
If no format is specified, then the default format for provided OS locale will be used.

Syntax: ```date( <date-as-string> [<format>] [,<timezone>] )```

Available since:

#### Example

```sql
SELECT FROM Account WHERE created <= date('2012-07-02', 'yyyy-MM-dd')
```
---
### sysdate()

Returns the current date time. If executed with no parameters, it returns a Date object, otherwise a string with the requested format/timezone.

Syntax: ```sysdate( [<format>] [,<timezone>] )```

Available since:

#### Example

```sql
SELECT sysdate('dd-MM-yyyy') FROM Account
```
---
### format()

Formats a value using the [String.format()](http://download.oracle.com/javase/1.5.0/docs/api/java/lang/String.html) conventions. 
Look [here for more information](http://download.oracle.com/javase/1.5.0/docs/api/java/util/Formatter.html#syntax).

Syntax: ```format( <format> [,<arg1> ](,<arg-n>]*.md)```

Available since:

#### Example

```sql
SELECT format("%d - Mr. %s %s (%s)", id, name, surname, address) FROM Account
```
---
### distance()

Syntax: ```distance( <x-field>, <y-field>, <x-value>, <y-value> )```

Returns the distance between two points in the globe using the Haversine algorithm. Coordinates must be as degrees.

Available since 

#### Example

```sql
SELECT FROM POI WHERE distance(x, y, 52.20472, 0.14056 ) <= 30
```
---
### distinct()

Syntax: ```distinct(<property>)```

Retrieves only unique data entries depending on the property you have specified as argument. 
The main difference compared to standard SQL DISTINCT is that with Xodus, a function with parenthesis and only one property can be specified.

Available since:

#### Example

```sql
SELECT distinct(name) FROM City
```
---
### unionall()

Syntax: ```unionall(<property> [,<property-n>]*)```

Works as aggregate or inline. If only one argument is passed then aggregates, otherwise executes and returns a UNION of all the collections received as parameters.
Also works with no collection values.

Available since: 

#### Example

```sql
SELECT unionall(friends) FROM profile
```

```sql
select unionall(inLink, outLink) from Entity where label = 'test'
```
---
### intersect()

Syntax: ```intersect(<property> [,<property-n>]*)```

Works as aggregate or inline. If only one argument is passed then it aggregates, otherwise executes and returns the INTERSECTION of the collections
received as parameters.

Available since:

#### Example

```sql
SELECT intersect(friends) FROM profile WHERE jobTitle = 'programmer'
```

```sql
SELECT intersect(inLink, outLink) FROM Entity
```
---
### difference()

Syntax: ```difference(<property-1> [,<property-n>]*)```

Works as aggregate or inline. If only one argument is passed then it aggregates, 
otherwise it executes and returns the DIFFERENCE between the collections received as parameters.

Available since:

#### Example

```sql
SELECT difference(tags) FROM book
```

```sql
SELECT difference(inLink, outLink) FROM OGraphVertex
```
---

### symmetricDifference()

Syntax: ```symmetricDifference(<property> [,<property-n>]*)```

Works as aggregate or inline. If only one argument is passed then it aggregates, 
otherwise executes and returns the SYMMETRIC DIFFERENCE between the collections received as parameters.

Available since:

#### Example

```sql
SELECT difference(tags) FROM book
```

```sql
SELECT difference(inLink, outLink) FROM Entity
```

---

### set()

Adds a value to a set. The first time the set is created. If ```<value>``` is a collection, then is merged with the set, otherwise ```<value>``` is added to the set.

Syntax: ```set(<property>)```

Available since:

#### Example

```sql
SELECT name, set(roles.name) AS roles FROM User
```
---
### list()

Adds a value to a list. The first time the list is created. If ```<value>``` is a collection, then is merged with the list, 
otherwise ```<value>``` is added to the list.

Syntax: ```list(<property>)```

Available since:

#### Example

```sql
SELECT name, list(roles.name) AS roles FROM OUser
```
---
### map()

Adds a value to a map. The first time the map is created. If ```<value>``` is a map, then is merged with the map, 
otherwise the pair ```<key>``` and ```<value>``` is added to the map as new entry.

Syntax: ```map(<key>, <value>)```

Available since:

#### Example

```sql
SELECT map(name, roles.name) FROM OUser
```
---
### traversedEntity()

Returns the traversed entity(s) in Traverse commands.

Syntax: ```traversedEntity(<index> [,<items>])```

Where:
- ```<index>``` is the starting item to retrieve. Value >= 0 means absolute position in the traversed stack. 
- 0 means the first entity. Negative values are counted from the end: -1 means last one, -2 means the record before last one, etc.
- ```<items>```, optional, by default is 1. If >1 a collection of items is returned

Available since:

#### Example

Returns last traversed item of TRAVERSE command:
```sql
SELECT traversedEntity(-1) FROM ( TRAVERSE out() FROM #34-3232 WHILE $depth <= 10 )
```

Returns last 3 traversed items of TRAVERSE command:
```sql
SELECT traversedEntity(-1, 3) FROM ( TRAVERSE out() FROM #34-3232 WHILE $depth <= 10 )
```
---

### mode()

Returns the values that occur with the greatest frequency. Nulls are ignored in the calculation.

Syntax: ```mode(<property>)```

Available since:

#### Example

```sql
SELECT mode(salary) FROM Account
```
---
### median()

Returns the middle value or an interpolated value that represent the middle value after the values are sorted. Nulls are ignored in the calculation.

Syntax: ```median(<property>)```

Available since:

#### Example

```sql
select median(salary) from Account
```
---
### percentile()

Returns the nth percentiles (the values that cut off the first n percent of the property values when it is sorted in ascending order). 
Nulls are ignored in the calculation.

Syntax: ```percentile(<property> [, <quantile-n>]*)```

The quantiles have to be in the range 0-1

Available since:

#### Examples

```sql
SELECT percentile(salary, 0.95) FROM Account
SELECT percentile(salary, 0.25, 0.75) AS IQR FROM Account
```
---
### variance()

Returns the middle variance: the average of the squared differences from the mean. Nulls are ignored in the calculation.

Syntax: ```variance(<field>)```

Available since:

#### Example

```sql
SELECT variance(salary) FROM Account
```
---
### stddev()

Returns the standard deviation: the measure of how spread out values are. Nulls are ignored in the calculation.

Syntax: ```stddev(<field>)```

Available since:

#### Example

```sql
SELECT stddev(salary) FROM Account
```
---
### uuid()

Generates a UUID as a 128-bits value using the Leach-Salz variant. For more information look at: http://docs.oracle.com/javase/6/docs/api/java/util/UUID.html.

Available since:

Syntax: ```uuid()```

#### Example

Insert a new record with an automatic generated id:

```sql
INSERT INTO Account SET id = UUID()
```
---
### strcmpci()

Compares two string ignoring case. Return value is -1 if first string ignoring case is less than second, 0 if strings ignoring case are equals, 1 if second string ignoring case is less than first one.
Before comparison both strings are transformed to lowercase and then compared.
  
Available since:

Syntax: ```strcmpci(<first_string>, <second_string>)```

#### Example

Select all entities where state name ignoring case is equal to "washington" 

```sql
SELECT * from State where strcmpci("washington", name) = 0
```

---

## Custom functions

The SQL engine can be extended with custom functions written with a Scripting language or via Java.
