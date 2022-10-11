# SQL - `MATCH`

Queries the database in a declarative manner, using pattern matching.

**Simplified Syntax**


```
MATCH 
  {
    [type: <entity-type>], 
    [as: <alias>], 
    [where: (<whereCondition>)]
  }
  .<functionName>(){
    [type: <entity-type>], 
    [as: <alias>], 
    [where: (<whereCondition>)], 
    [while: (<whileCondition>)],
    [maxDepth: <number>],    
    [depthAlias: <identifier> ], 
    [pathAlias: <identifier> ],     
    [optional: (true | false)]
  }*
  [,
    [NOT]
    {
      [as: <alias>], 
      [type: <entity-type>], 
      [where: (<whereCondition>)]
    }
    .<functionName>(){
      [type: <entity-type>], 
      [as: <alias>], 
      [where: (<whereCondition>)], 
      [while: (<whileCondition>)],
      [maxDepth: <number>],    
      [depthAlias: <identifier> ], 
      [pathAlias: <identifier> ],     
      [optional: (true | false)]
    }*
  ]*
RETURN [DISTINCT] <expression> [ AS <alias> ] [, <expression> [ AS <alias> ]]*
GROUP BY <expression> [, <expression>]*
ORDER BY <expression> [, <expression>]*
SKIP <number>
LIMIT <number>
```


- **`<entity-type>`** Defines a valid target entity type.
- **`as: <alias>`** Defines an alias for a node in the pattern.
- **`<whereCondition>`** Defines a filter condition to match a node in the pattern.  It supports the normal SQL [`WHERE`](SQL-Where.md) clause.
You can also use the `$currentMatch` and `$matched` [context variables](#context-variables).
- **`<functionName>`** Defines a function to represent the connection between two nodes:  `out()`, `in()`, `both()`.
For out(), in(), both() also a shortened *arrow* syntax is supported: 
  - `{...}.out(){...}` can be written as `{...}-->{...}`
  - `{...}.out("linkName"){...}` can be written as `{...}-linkName->{...}`
  - `{...}.in(){...}` can be written as `{...}<--{...}`
  - `{...}.in("linkName"){...}` can be written as `{...}<-linkName-{...}`
  - `{...}.both(){...}` can be written as `{...}--{...}`
  - `{...}.both("linkName"){...}` can be written as `{...}-linkName-{...}`
- **`<whileCondition>`** Defines a condition that the statement must meet to allow the traversal of this path. 
It supports the normal SQL [`WHERE`](SQL-Where.md) clause.  
You can also use the `$currentMatch`, `$matched` and `$depth` [context variables](#context-variables).  
For more information, see [Deep Traversal While Condition](#deep-traversal), below.
- **`<maxDepth>`** Defines the maximum depth for this single path.
- **`<depthAlias>`** This is valid only if you have a `while` or a `maxDepth`.
It defines the alias to be used to store the depth of this traversal. This alias can be used in the `RETURN` block to retrieve the depth of current traversal.
- **`<pathAlias>`** This is valid only if you have a `while` or a `maxDepth`. 
It defines the alias to be used to store the elements traversed to reach this alias.
This alias can be used in the `RETURN` block to retrieve the elements traversed to reach this alias.
- **`RETURN <expression> [ AS <alias> ]`** Defines elements in the pattern that you want to be returned.  It can use one of the following:
  - Aliases defined in the `as:` block.
  - `$matches` Indicating all defined aliases.
  - `$paths` Indicating the full traversed paths.
  - `$elements` Indicating that all the elements that would be returned by the `$matches` have to be returned flattened, without duplicates.
  - `$pathElements`Indicating that all the elements that would be returned by the `$paths` have to be returned flattened, without duplicates.
- **`optional`**  if set to true, allows to evaluate and return a pattern even if that particular node does not match the pattern itself 
(ie. there is no value for that node in the pattern).
In current version, optional nodes are allowed only on right terminal nodes, eg. `{} --> {optional:true}` is allowed, 
`{optional:true} <-- {}` is not.
- **`NOT` patterns** Together with normal patterns, you can also define negative patterns. 
A result will be returned only if it also DOES NOT match any of the negative patterns, ie. if it matches at least one of the negative patterns it won't be returned.


### Deep Traversal


Match path items act in a different manners, depending on whether or not you use `while:` conditions in the statement.

For instance, consider the following object graph:

``` 
[name='a'] -friendOf-> [name='b'] -friendOf-> [name='c']
```

Running the following statement on this object graph only returns `b`:

<pre>
<code class="lang-sql userinput">MATCH {type: Person, where: (name = 'a')}.out("friendOf")
          {as: friend} RETURN friend</code>

--------
 friend 
--------
 b
--------
</pre>

What this means is that it traverses the path item `out("friendOf")` exactly once.  It only returns the result of that traversal.

If you add a ```while``` condition:

<pre>
  <code class="lang-sql userinput">MATCH {type: Person, where: (name = 'a')}.out("friendOf")
          {as: friend, while: ($depth < 2)} RETURN friend</code>

---------
 friend 
---------
 a
 b
---------
</pre>

Including a `while:` condition on the match path item causes Xodus to evaluate this item as zero to *n* times. 
That means that it returns the starting node, (`a`, in this case), as the result of zero traversal.

To exclude the starting point, you need to add a `where:` condition, such as:

<pre>
  <code class="lang-sql userinput">MATCH {type: Person, where: (name = 'a')}.out("friendOf")
          {as: friend, while: ($depth < 2) where: ($depth > 0)} 
		  RETURN friend</code>
</pre>

As a general rule,
- **`while` Conditions:** Define this if it must execute the next traversal, (it evaluates at level zero, on the origin node).
- **`where` Condition:** Define this if the current element, (the origin node at the zero iteration the right node on the iteration is greater than zero),
must be returned as a result of the traversal.

For instance, suppose that you have a genealogical tree.  
In the tree, you want to show a person, grandparent and the grandparent of that grandparent, and so on.
The result: saying that the person is at level zero, parents at level one, grandparents at level two, etc., you would see all ancestors on even levels.
That is, `level % 2 == 0`.

To get this, you might use the following query:

<pre>
  <code class="lang-sql userinput">MATCH {type: Person, where: (name = 'a')}.out("parent")
          {as: ancestor, while: (true), where: ($depth % 2 = 0)} 
		  RETURN ancestor</code>
</pre>


**Examples**

The following examples are based on sample data-set from the entity type `People`:

- Find all people with the name John:

<pre>
    <code class="lang-sql userinput">
    MATCH {type: Person, as: people, where: (name = 'John')} 
            RETURN people</code>

  ---------
    people 
  ---------
    #12-0
    #12-1
  ---------
  </pre>

- Find all people with the name John and the surname Smith:

  <pre>
    <code class="lang-sql userinput">
    MATCH  {type: Person, as: people, where: (name = 'John' AND surname = 'Smith')} 
	    RETURN people</code>

  -------
  people
  -------
   #12-1
  -------
  </pre>


- Find people named John with their friends:

  <pre>
    <code class="lang-sql userinput">
    MATCH {type: Person, as: person, where: (name = 'John')}.both('friend') {as: friend} 
            RETURN person, friend</code>

  --------+---------
   person | friend 
  --------+---------
   #12-0  | #12-1
   #12-0  | #12-2
   #12-0  | #12-3
   #12-1  | #12-0
   #12-1  | #12-2
  --------+---------
  </pre>
 

- Find friends of friends:

  <pre>
    <code class="lang-sql userinput">
    MATCH {type: Person, as: person, where: (name = 'John' AND surname = 'Doe')}
		    .both('friend').both('friend') {as: friendOfFriend} 
		RETURN person, friendOfFriend</code>

  --------+----------------
   person | friendOfFriend 
  --------+----------------
   #12-0  | #12-0
   #12-0  | #12-1
   #12-0  | #12-2
   #12-0  | #12-3
   #12-0  | #12-4
  --------+----------------
  </pre>
  
  
- Find people, excluding the current user:
  
  <pre>
    <code class="lang-sql userinput">MATCH {type: Person, as: person, where: (name = 'John' AND 
            surname = 'Doe')}.both('friend').both('friend'){as: friendOfFriend,
			where: ($matched.person != $currentMatch)} 
			RETURN person, friendOfFriend</code>

  --------+----------------
   person | friendOfFriend
  --------+----------------
   #12-0  | #12-1
   #12-0  | #12-2
   #12-0  | #12-3
   #12-0  | #12-4
  --------+----------------
  </pre>
  
- Find friends of friends to the sixth degree of separation:
  
  <pre>
    <code class="lang-sql userinput">MATCH {type: Person, as: person, where: (name = 'John' AND 
            surname = 'Doe')}.both('friend'){as: friend, 
			where: ($matched.person != $currentMatch and $depth > 1), while: ($depth < 6)} 
			RETURN person, friend</code>

  --------+---------
   person | friend
  --------+---------
   #12-0  | #12-1
   #12-0  | #12-2
   #12-0  | #12-3
   #12-0  | #12-4
  --------+---------
  </pre>


- Finding friends of friends to six degrees of separation, since a particular date:

  <pre>
    <code class="lang-sql userinput">MATCH {type: Person, as: person, 
            where: (name = 'John')}.both('friend').both("friend") {as: friend, 
			while: ($depth < 6) where: (date < ? and $depth > 0)} RETURN person, friend</code>
  </pre>
  
  In this case, the condition ``$depth < 6`` refers to traversing all the blocks ``both('friend')`` six times.
  `?` - is a placeholder for a date.

- Find friends of my friends who are also my friends, using multiple paths:

  <pre>
   <code class="lang-sql userinput">MATCH {type: Person, as: person, where: (name = 'John' AND 
            surname = 'Doe')}.both('friend').both('friend'){as: friend},
			{ as: person }.both('friend'){ as: friend } 
			RETURN person, friend</code>

  --------+--------
   person | friend
  --------+--------
   #12-0  | #12-1
   #12-0  | #12-2
  --------+--------
  </pre>
  
  In this case, the statement matches two expression: the first to friends of friends, the second to direct friends.
  Each expression shares the common aliases (`person` and `friend`). 
  To match the whole statement, the result must match both expressions, where the alias values for the first expression are the same as that of the second.

- Find common friends of John and Jenny:

  <pre>
    <code class="lang-sql userinput">MATCH {type: Person, where: (name = 'John' AND 
            surname = 'Doe')}.both('friend'){as: friend}.both('friend')
			{type: Person, where: (name = 'Jenny')} RETURN friend</code>

  --------
   friend
  --------
   #12-1
  --------
  </pre>
  
  The same, with two match expressions:

  <pre>
   <code class="lang-sql userinput">MATCH { type : Person, where: (name = 'John' AND 
            surname = 'Doe')}.both('friend'){as: friend}, 
			{type: Person, where: (name = 'Jenny')}.both('friend')
			{as: friend} RETURN friend</code>
  </pre>


## DISTINCT

To have unique, distinct records as a result, you have to specify the DISTINCT keyword in the RETURN statement.

Example: suppose you have a dataset made like following:

```sql
   INSERT INTO Person SET name = 'John', surname = 'Smith';
   INSERT INTO Person SET name = 'John', surname = 'Harris'
   INSERT INTO Person SET name = 'Jenny', surname = 'Rose'
```   

This is the result of the query without a DISTINCT clause:

  <pre>
    <code class="lang-sql userinput">MATCH {type: Person, as:p} RETURN p.name as name</code>
    
  --------
   name
  --------
   John
  --------
   John
  --------
   Jenny
  --------
  </pre>


And this is the result of the query with a DISTINCT clause:

  <pre>
    <code class="lang-sql userinput">MATCH {type: Person, as:p} RETURN DISTINCT p.name as name</code>

  --------
   name
  --------
   John
  --------
   Jenny
  --------
  </pre>
  
## Context Variables

When running these queries, you can use any of the following context variables:

| Variable | Description |
|---|---|
|`$matched`| Gives the currently matched entities. 
You can use this in the `where:` and `while:` conditions to refer to current partial matches or as part of the `RETURN` value.|
|`$currentMatch`| Gives the current entity during the match.|
|`$depth`| Gives the traversal depth, following a single path item where a `while:` condition is defined.|


## Use Cases


### Expanding Attributes

You can run this statement as a sub-query inside of another statement. 
Doing this allows you to obtain details and aggregate data from the inner [`SELECT`](SQL-Query.md) query.

<pre>
  <code class="lang-sql userinput">SELECT person.name AS name, person.surname AS surname,
          friend.name AS friendName, friend.surname AS friendSurname
		  FROM (MATCH {type : Person, as: person,
		  where: (name = 'John')}.both('friend'){as: friend}
		  RETURN person, friend) </code>

--------+----------+------------+---------------
 name   | surname  | friendName | friendSurname
--------+----------+------------+---------------
 John   | Doe      | John       | Smith
 John   | Doe      | Jenny      | Smith
 John   | Doe      | Frank      | Bean
 John   | Smith    | John       | Doe
 John   | Smith    | Jenny      | Smith
--------+----------+------------+---------------
</pre>

As an alternative, you can use the following:

<pre>
   <code class="lang-sql userinput">MATCH {type : Person, as: person,
		  where: (name = 'John')}.both('friend'){as: friend}
		  RETURN 
		  person.name as name, person.surname as surname, 
		  friend.name as firendName, friend.surname as friendSurname</code>

--------+----------+------------+---------------
 name   | surname  | friendName | friendSurname
--------+----------+------------+---------------
 John   | Doe      | John       | Smith
 John   | Doe      | Jenny      | Smith
 John   | Doe      | Frank      | Bean
 John   | Smith    | John       | Doe
 John   | Smith    | Jenny      | Smith
--------+----------+------------+---------------
</pre>
 



### Incomplete Hierarchy

Consider building a database for a company that shows a hierarchy of departments within the company.  For instance,

```
           [manager] department        
          (employees in department)    
                                       
                                       
                [m0]0                   
                 (e1)                  
                 /   \                 
                /     \                
               /       \               
           [m1]1        [m2]2
          (e2, e3)     (e4, e5)        
             / \         / \           
            3   4       5   6          
          (e6) (e7)   (e8)  (e9)       
          /  \                         
      [m3]7    8                       
      (e10)   (e11)                    
       /                               
      9                                
  (e12, e13)                         
```

This loosely shows that,
- Department `0` is the company itself, manager 0 (`m0`) is the CEO
- `e10` works at department `7`, his manager is `m3`
- `e12` works at department `9`, this department has no direct manager, so `e12`'s manager is `m3` (the upper manager)


In this case, you would use the following query to find out who's the manager to a particular employee:

<pre>
  <code class="lang-sql userinput">
       SELECT EXPAND(manager) FROM (MATCH {type : Employee, 
          where: (name = ?)}.out('worksAt').out('parentDepartment')
		  {while: (out('manager').size() == 0), 
		  where: (out('manager').size() > 0)}.out('manager')
		  {as: manager} RETURN manager)</code>
</pre>



## Best practices

Queries can involve multiple operations, based on the domain model and use case. 
In some cases, like projection and aggregation, you can easily manage them with a [`SELECT`](SQL-Query.md) query.  
With others, such as pattern matching and deep traversal, [`MATCH`](SQL-Match.md) statements are more appropriate.

Use [`SELECT`](SQL-Query.md) and [`MATCH`](SQL-Match.md) statements together (that is, through sub-queries), to give each statement the correct responsibilities. 

### Filtering Entity Attributes for a Single Type

Filtering based on entity attributes for a single type is a trivial operation through both statements.  
That is, finding all people named John can be written as:

```sql
  SELECT FROM Person WHERE name = 'John'
```

You can also write it as,

```sql
MATCH {type: Person, as: person, where: (name = 'John')} 
          RETURN person
```


The efficiency remains the same.  
With [`SELECT`](SQL-Query.md), you obtain expanded records, while with [`MATCH`](SQL-Match.md), you only obtain the entity id's.


### Filtering on Entity Attributes of Connected Elements

Filtering based on the enitty attributes of connected elements, such as neighboring entities, 
can grow trick when using [`SELECT`](SQL-Query.md), while with [`MATCH`](SQL-Match.md) it is simple.

For instance, find all people living in Rome that have a friend called John. 
There are three different ways you can write this, using [`SELECT`](SQL-Query.md):

```sql
   SELECT FROM Person WHERE both('friend').name CONTAINS 'John'
          AND out('livesIn').name CONTAINS 'Rome'

   SELECT FROM (SELECT both('friend') FROM Person WHERE name
          'John') WHERE out('livesIn').name CONTAINS 'Rome'

   SELECT FROM (SELECT in('livesIn') FROM City WHERE name = 'Rome')
          WHERE BOTH('friend').name CONTAINS 'John'
```
 
Using a [`MATCH`](SQL-Match.md) statement, the query becomes:

<pre>
  <code class="lang-sql userinput">MATCH {type: Person, where: (name = 'John')}.both("friend")
          {as: result}.out('livesIn'){type: City, where: (name = 'Rome')}
		  RETURN result</code>
</pre>


### `TRAVERSE` Alternative

There are similar limitations to using [`TRAVERSE`](SQL-Traverse.md).  
You may benefit from using [`MATCH`](SQL-Match.md) as an alternative.

For instance, consider a simple [`TRAVERSE`](SQL-Traverse.md) statement, like:

<pre>
  <code class="lang-sql userinput">TRAVERSE out('friend') FROM (SELECT FROM Person WHERE name = 'John') 
          WHILE $depth < 3</code>
</pre>


Using a [`MATCH`](SQL-Match.md) statement, you can write the same query as:

<pre>
  <code class="lang-sql userinput">MATCH {type: Person, where: (name = 'John')}.both("friend")
          {as: friend, while: ($depth < 3)} RETURN friend</code>
</pre>


### Projections and Grouping Operations

Projections and grouping operations are better expressed with a [`SELECT`](SQL-Query.md) query.  
If you need to filter and do projection or aggregation in the same query, you can use [`SELECT`](SQL-Query.md) 
and [`MATCH`](SQL-Match.md) in the same statement.

This is particular important when you expect a result that contains attributes from different connected entities (cartesian product).  
For instance, to retrieve names and their friends:

```sql
  SELECT person.name AS name, friend.name 
          AS friend FROM (MATCH {type: Person, as: person}.both('friend') {as: friend, 
		  where: ($matched.person != $currentMatch)} 
		  RETURN person, friend)
```

The same can be also achieved with the MATCH only:

```sql
   MATCH {type: Person, as: person}.both('Friend') {as: friend, 
		  where: ($matched.person != $currentMatch)} 
		  RETURN person.name as name, friend.name as friend
```

### RETURN expressions

In the RETURN section you can use:

**multiple expressions**, with or without an alias (if no alias is defined, Xodus will generate a default alias for you), comma separated

```
MATCH 
  {type: Person, as: person}
  .both(){as: friend, where: ($matched.person != $currentMatch)} 
RETURN person, friend

result: 

| person | friend |
--------------------------------
| #12-0  |  #12-2  |
| #12-0  |  #12-3  |
| #12-1  |  #12-3  |
```

```
MATCH 
  {type: Person, as: person}
  .both(){as: friend, where: ($matched.person != $currentMatch)} 
RETURN person.name as name, friend.name as friend

result: 

| name | friend |
-----------------
| John | Frank  |
| John | Jenny  |
| Joe  | Jenny  |

```

```
MATCH 
  {type: Person, as: person}
  .both(){as: friend, where: ($matched.person != $currentMatch)} 
RETURN person.name + " is a friend of " + friend.name as friends

result: 

| friends                    |
------------------------------
| John is a friend of Frank  |
| John is a friend of Jenny  |
| Joe is a friend of Jenny   |

```

**$matches**, to return all the patterns that match current statement. 
Each row in the result set will be a single pattern, containing only nodes in the statement that have an `as:` defined

```
MATCH 
  {type: Person, as: person}
  .both(){as: friend, where: ($matched.person != $currentMatch)} 
RETURN $matches

result: 

| person |  friend | 
--------------------
| #12-0  |  #12-2  |
| #12-0  |  #12-3  |
| #12-1  |  #12-3  |

```

**$paths**, to return all the patterns that match current statement. 
Each row in the result set will be a single pattern, containing all th nodes in the statement. 
For nodes that have an `as:`, the alias will be returned, for the others a default alias is generated 
(automatically generated aliases start with `$XODUS_DEFAULT_ALIAS_`)

```
MATCH 
  {type: Person, as: person}
  .both(){where: ($matched.person != $currentMatch)} 
RETURN $paths

result: 

| person | $XODUS_DEFAULT_ALIAS_0  |
------------------------------------
| #12-0  | #12-2                   | 
| #12-0  | #12-3                   |
| #12-1  | #12-3                   |
```

**$elements** - the same as `$matches`, but for each node present in the pattern, a single row is created in the result set (no duplicates)

```
MATCH 
  {type: Person, as: person}
  .both(){as: friend, where: ($matched.person != $currentMatch)} 
RETURN $elements

result: 

| @eid   |  @type | name   |  .....   |
----------------------------------------
| #12-0  |  Person | John   |  .....   |
| #12-1  |  Person | Joe    |  .....   |
| #12-2  |  Person | Frank  |  .....   |
| #12-3  |  Person | Jenny  |  .....   |

```

**$pathElements** the same as `$paths`, but for each node present in the pattern, a single row is created in the result set (no duplicates)

```
MATCH 
  {type: Person, as: person}
  .both(){as: friend, where: ($matched.person != $currentMatch)} 
RETURN $pathElements

result: 

| @eid   |  @type | name   | since  |  .....   |
-------------------------------------------------
| #12-0  |  Person | John   |        |  .....   |
| #12-1  |  Person | Joe    |        |  .....   |
| #12-2  |  Person | Frank  |        |  .....   |
| #12-3  |  Person | Jenny  |        |  .....   |
| #13-0  |  Friend |        |  2015  |  .....   |
| #13-1  |  Friend |        |  2015  |  .....   |
| #13-2  |  Friend |        |  2016  |  .....   |

```


### Arrow notation

`out()`, `in()` and `both()` operators can be replaced with arrow notation `-->`, `<--` and `--`

Eg. the query 

<pre>
<code class="lang-sql userinput">
MATCH {type: E, as: a}.out(){}.out(){}.out(){as:b}
RETURN a, b
</code>
</pre>

can be written as

<pre>
<code class="lang-sql userinput">
MATCH {type: E, as: a} --> {} --> {} --> {as:b}
RETURN a, b
</code>
</pre>


Eg. the query (things that belong to friends)

<pre>
<code class="lang-sql userinput">
MATCH {type: Person, as: a}.out('friend'){as:friend}.in('belongsTo'){as:b}
RETURN a, b
</code>
</pre>

can be written as

<pre>
<code class="lang-sql userinput">
MATCH {type: Person, as: a}  -friend-> {as:friend} <-belongsTo- {as:b}
RETURN a, b
</code>
</pre>

Using arrow notation the curly braces are mandatory on both sides. eg:

<pre>
<code class="lang-sql userinput">
MATCH {type: Person, as: a} --> {} --> {as:b} RETURN a, b  //is allowed

MATCH {type: Person, as: a} --> --> {as:b} RETURN a, b  //is NOT allowed

MATCH {type: Person, as: a}.out().out(){as:b} RETURN a, b  //is allowed

MATCH {type: Person, as: a}.out(){}.out(){as:b} RETURN a, b  //is allowed
</code>
</pre>

### Negative (NOT) patterns

Together with normal patterns, you can also define negative patterns.
A result will be returned only if it also DOES NOT match any of the negative patterns, 
ie. if the result matches at least one of the negative patterns it won't be returned.

As an example, consider the following problem: given a social network, choose a single person
and return all the people that are friends of their friends, but that are not their direct friends.

The pattern can be calculated as follows:

<pre>
<code class="lang-sql userinput">
MATCH
  {type:Person, as:a, where:(name = "John")} -friendOf-> {as:b} -friendOf-> {as:c},
  NOT {as:a} -friendOf-> {as:c}
RETURN c.name
</code>
</pre>
