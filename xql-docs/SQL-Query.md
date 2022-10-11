
# SQL - `SELECT`

Xodus supports the SQL language to execute queries against the database engine. 
For more information, see [operators](SQL-Where.md#operators) and [functions](SQL-Where.md#functions).  
For more information on the differences between this implementation and the SQL-92 standard, please refer to [this](SQL-Introduction.md) section.

**Syntax**:

```sql
SELECT [ <Projections> ] [ FROM <Target> [ LET <Assignment>* ] ]
    [ WHERE <Condition>* ]
    [ GROUP BY <Property>* ]
    [ ORDER BY <Property>* [ ASC|DESC ] * ]
    [ UNWIND <Property>* ]
    [ SKIP <SkipEnitites> ]
    [ LIMIT <MaxEntities> ]
    [ TIMEOUT <Timeout> [ <STRATEGY> ]
    [ NOCACHE ]
```

- **[`<Projections>`](SQL-Query.md#projections)** Indicates the data you want to extract from the query as the result-set.  Note: In Xodus, this part is optional.  
In the projections you can define aliases for single properties, using the `AS` keyword; 
in current release aliases cannot be used in the WHERE condition, GROUP BY and ORDER BY (they will be evaluated to null)
- **`FROM`** Designates the object to query.  This can be a entity type or single entity id, set of entity ids.
- **[`WHERE`](SQL-Where.md)** Designates conditions to filter the result-set.
- **[`LET`](SQL-Query.md#let-block)** Binds context variables to use in projections, conditions or sub-queries.
- **`GROUP BY`** Designates properties on which to group the result-set. 
- **`ORDER BY`** Designates the property with which to order the result-set.  
Use the optional `ASC` and `DESC` operators to define the direction of the order. 
The default is ascending.  Additionally, if you are using a [projection](SQL-Query.md#projections), you need to include the `ORDER BY` property in the projection. 
Note that ORDER BY works only on projection properties (properties that are returned in the result set) not on LET variables.
- **[`UNWIND`](SQL-Query.md#unwinding)** Designates the property on which to unwind the collection.
- **`SKIP`** Defines the number of records you want to skip from the start of the result-set.
You may find this useful in [pagination](Pagination.md), when using it in conjunction with `LIMIT`.
- **`LIMIT`** Defines the maximum number of records in the result-set. 
You may find this useful in [pagination](Pagination.md), when using it in conjunction with `SKIP`. 
- **`TIMEOUT`** Defines the maximum time in milliseconds for the query.  By default, queries have no timeouts.
If you don't specify a timeout strategy, it defaults to `EXCEPTION`.  These are the available timeout strategies:
  - `RETURN` Truncate the result-set, returning the data collected up to the timeout.
  - `EXCEPTION` Raises an exception.
- **`NOCACHE`** Defines whether you want to avoid using the cache.

**Examples**:

- Return all entities of the enittye type `Person`, where the name starts with `Luk`:

  <pre>
    <code class="lang-sql userinput">SELECT FROM Person WHERE name LIKE 'Luk%'</code>
  </pre>

  Alternatively, you might also use either of these queries:

  <pre>
    <code class="lang-sql userinput">SELECT FROM Person WHERE name.left(3) = 'Luk'</code>
    <code class="lang-sql userinput">SELECT FROM Person WHERE name.substring(0,3) = 'Luk'</code>
  </pre>
  
- Return all entities of the type `AnimalType` where the collection `races` contains at least one entry where the first character is `e`, ignoring case:

  <pre>
    <code class="lang-sql userinput">SELECT FROM animaltype WHERE races CONTAINS( name.toLowerCase().subString(
            0, 1) = 'e' )</code>
  </pre>

- Return all entries of type `AnimalType` where the collection `races` contains at least one entry with names `European` or `Asiatic`:

  <pre>
    <code class="lang-sql userinput">SELECT * FROM AnimalType WHERE races CONTAINS(name in ['European',
            'Asiatic'])</code>
  </pre>

- Return all entities or type `Profile` where any property contains the word `danger`:

  <pre>
    <code class="lang-sql userinput">SELECT FROM Profile WHERE ANY() LIKE '%danger%'</code>
  </pre>


- Return all entities of type `Profile`, ordered by the field `name` in descending order:

  <pre>
    <code class="lang-sql userinput">SELECT FROM Profile ORDER BY name DESC</code>
  </pre>

- Return the number of entities of type `Account` per city:

  <pre>
    <code class="lang-sql userinput">SELECT SUM(*) FROM Account GROUP BY city</code>
  </pre>

- Return only a limited set of entities:

  <pre>
    <code class="lang-sql userinput">SELECT FROM [#10-3, #10-4, #10-5]</code>
  </pre>

- Return three properties from the entity of type `Profile`:

  <pre>
    <code class="lang-sql userinput">SELECT nick, followings, followers FROM Profile</code>
  </pre>

- Return the property `name` in uppercase and the property country name of the linked city of the address:

  <pre>
    <code class="lang-sql userinput">SELECT name.toUppercase(), address.city.country.name FROM Profile</code>
  </pre>

- Return entities of type `Profile` in descending order of their creation:

  <pre>
    <code class="lang-sql userinput">SELECT FROM Profile ORDER BY @eid DESC</code>
  </pre>
  
- Return value of `email` which is stored in a property `data`  of type `Person`, where the name starts with `Luk`:

  <pre>
  orientdb> <code class="lang-sql userinput">SELECT data.email FROM Person WHERE name LIKE 'Luk%'</code>
  </pre>

## Projections

In the standard implementations of SQL, projections are mandatory. 
In Xodus, the omission of projections translates to its returning the entire entity. 
That is, it reads no projection as the equivalent of the `*` wildcard.

<pre>
   <code class="lang-sql userinput">SELECT FROM Account</code>
</pre>

<pre> 
  <code class="lang-sql userinput">SELECT name, age FROM Account</code>
</pre>

The naming convention for the returned fields are:
- Property name for plain properties, like `invoice` becoming `invoice`.
- First property name for chained properties, like `invoice.customer.name` becoming `invoice`.
- Function name for functions, like `MAX(salary)` becoming `max`.

In the event that the target field exists, it uses a numeric progression.  For instance,

<pre>
  <code class="lang-sql userinput">SELECT MAX(incoming), MAX(cost) FROM Balance</code>

------+------
 max  | max2
------+------
 1342 | 2478
------+------
</pre>

To override the display for the field names, use the `AS`.

<pre>
  <code class="lang-sql userinput">SELECT MAX(incoming) AS max_incoming, MAX(cost) AS max_cost FROM Balance</code>

---------------+----------
 max_incoming  | max_cost
---------------+----------
 1342          | 2478
---------------+----------
</pre>

With the dollar sign `$`, you can access the context variables.
Each time you run the command, Xouds accesses the context to read and write the variables. 
For instance, say you want to display the path and depth levels up to the fifth of a [`TRAVERSE`](SQL-Traverse.md) on all entities in the `Movie` type.

<pre>
  <code class="lang-sql userinput">SELECT $path, $depth FROM ( TRAVERSE * FROM Movie WHERE $depth <= 5 )</code>
</pre>


## `LET` Block

The `LET` block contains context variables to assign each time Xodus evaluates a record. 
It destroys these values once the query execution ends.  
You can use context variables in projections, conditions, and sub-queries.

### Assigning Fields for Reuse

Xodus allows for crossing relationships. 
In single queries, you need to evaluate the same branch of the nested relationship. 
This is better than using a context variable that refers to the full relationship.

<pre>
  <code class="lang-sql userinput">SELECT FROM Profile WHERE address.city.name LIKE '%Saint%"' AND 
          ( address.city.country.name = 'Italy' OR 
            address.city.country.name = 'France' )</code>
</pre>

Using the `LET` makes the query shorter and faster, because it traverses the relationships only once:

<pre>
orientdb> <code class="lang-sql userinput">SELECT FROM Profile LET $city = address.city WHERE $city.name LIKE 
          '%Saint%"' AND ($city.country.name = 'Italy' OR $city.country.name = 'France')</code>
</pre>

In this case, it traverses the path till `address.city` only once.

### Sub-query

The `LET` block allows you to assign a context variable to the result of a sub-query.

<pre>
  <code class="lang-sql userinput">SELECT FROM Document LET $temp = ( SELECT @eid, $depth FROM (TRAVERSE 
          out(), in() FROM $parent.current ) WHERE @type = 'Concept' AND 
          ( id = 'first concept' OR id = 'second concept' )) WHERE $temp.SIZE() > 0</code>
</pre>

### `LET` Block in Projection

You can use context variables as part of a result-set in [projections](#projections).  
For instance, the query below displays the city name from the previous example:

<pre>
  <code class="lang-sql userinput">SELECT $city.name FROM Profile LET $city = address.city WHERE $city.name 
          LIKE '%Saint%"' AND ( $city.country.name = 'Italy' OR 
          $city.country.name = 'France' )</code>
</pre>


## Unwinding
Xodus allows unwinding of collection properties and obtaining multiple records as a result, one for each element in the collection:

<pre>
<code class="lang-sql userinput">SELECT name, OUT("friend").name AS friendName FROM Person</code>

--------+-------------------
 name   | friendName
--------+-------------------
 'John' | ['Mark', 'Steve']
--------+-------------------
</pre>
 
If you want one record for each element in `friendName`, you can rewrite the query using `UNWIND`:

<pre>
 <code class="lang-sql userinput">SELECT name, OUT("friend").name AS friendName FROM Person UNWIND friendName</code>

--------+-------------
 name   | friendName
--------+-------------
 'John' | 'Mark'
 'John' | 'Steve'
--------+-------------
</pre>

>**NOTE**: For more information on other SQL commands, see [SQL Commands](SQL-Commands.md).


## Execution planning

For details about query execution planning, please refer to [SQL SELECT Execution](SQL-Select-Execution.md)
