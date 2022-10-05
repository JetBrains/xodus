# SQL - Filtering

The Where condition is shared among many SQL commands.

## Syntax

`[<item>] <operator> <item>`

## Items

And `item` can be:


|**What**|**Description**|**Example**|**Available since**|
|--------|---------------|-----------|-------------------|
|field|Entity field|where *price* > 1000000||
|field&lt;indexes&gt;|Entity property part. |where messages[title like '%Hi%'] or tags[0-3] IN 'Hello' and employees IS NOT NULL||
|entity attribute|Entity attribute name with @ as prefix|where *@type* = 'Profile'||
|any()|Represents any property of the entity. The condition is true if ANY of the properties match the condition|where *any()* like 'L%'||
|all()|Represents all the properties of the entity. The condition is true if ALL the propertes match the condition|where *all()* is null||
|[functions](SQL-Functions.md)|Any [function](SQL-Functions.md) between the defined ones|where sum(x) <= 30||
|[$variable](SQL-Where.md#variables)|Context variable prefixed with $|where $depth <= 3||


### Entity attributes


|Name|Description|Example|Available since|
|--------|---------------|-----------|-------------------|
|@this|returns the entity itself|select **@this.toJSON()** from Account||
|@eid|returns the entity id in the form &lt;entity-type-id&gt; - &lt;instance-id&gt;. *NOTE: using @eid in where condition slow down queries. Much better to use the entity id as target. Example: change this: select from Profile where @eid = #10-44 with this: select from #10-44 *|**@eid** = #11-0||
|@type|returns entity type name|**@type** = 'Profile'||

## Operators

### Conditional Operators

|Apply to|Operator|Description|Example|Available since|
|--------|---------------|-----------|-------------------|----|
|any|=|Equals to|name **=** 'Luke'||
|string|like|Similar to equals, but allow the wildcard '%' that means 'any'|name **like** 'Luk%'||
|any|<|Less than|age **<** 40||
|any|<=|Less than or equal to|age **<=** 40||
|any|>|Greater than|age **>** 40||
|any|>=|Greater than or equal to|age **>=** 40||
|any|<>|Not equals (same of !=)|age **<>** 40||
|any|BETWEEN|The value is between a range. It's equivalent to &lt;field&gt; &gt;= &lt;from-value&gt; AND &lt;field&gt; &lt;= &lt;to-value&gt;|price BETWEEN 10 and 30||
|any|IS|Used to test if a value is NULL|children **is** null||
|entity, string (as entity type name)|INSTANCEOF|Used to check if the entity is instance of entity type (including parents)|@this **instanceof** 'Customer' or @type **instanceof** 'Provider'||
|collection|IN|contains any of the elements listed|name **in** ['European','Asiatic']||
|collection|CONTAINS|true if the collection contains at least one element that satisfy the next condition. Condition can be a single item: in this case the behaviour is like the IN operator|children **contains** (name = 'Luke') - map.values() **contains** (name = 'Luke')||
|collection|CONTAINSALL|true if all the elements of the collection satisfy the next condition|children *containsAll* (name = 'Luke')||
|collection|CONTAINSANY|true if any the elements of the collection satisfy the next condition|children *containsAny* (name = 'Luke')||
|map|CONTAINSKEY|true if the map contains at least one key equals to the requested. You can also use map.keys() CONTAINS in place of it|connections *containsKey* 'Luke'||
|map|CONTAINSVALUE|true if the map contains at least one value equals to the requested. You can also use map.values() CONTAINS in place of it|connections *containsValue* 10||
|string|CONTAINSTEXT| true if property contains provided text snippet |text *containsText* 'jay'||
|string|MATCHES|Matches the string using a [http://www.regular-expressions.info/ Regular Expression]|text matches '\b[A-Z0-9.%+-]+@[A-Z0-9.-]+\.[A-Z]{2,4}\b'||

### Logical Operators

|Operator|Description|Example|Available since|
|--------|---------------|-----------|-------------------|
|AND|true if both the conditions are true|name = 'Luke' **and** surname like 'Sky%'||
|OR|true if at least one of the condition is true|name = 'Luke' **or** surname like 'Sky%'||
|NOT|true if the condition is false. NOT needs parenthesis on the right with the condition to negate|**not** ( name = 'Luke')||

### Mathematics Operators

|Apply to|Operator       |Description|Example            |Available since|
|--------|---------------|-----------|-------------------|---------------|
|Numbers|+|Plus|age + 34||
|Numbers|-|Minus|salary - 34||
|Numbers|\*|Multiply|factor \* 1.3||
|Numbers|/|Divide|total / 12||
|Numbers|%|Mod|total % 3||

### Methods

Also called "Field Operators", are [are treated on a separate page](SQL-Methods.md).

## Functions

All the [SQL functions are treated on a separate page](SQL-Functions.md).

## Variables

Xodus supports variables managed in the context of the command/query. By default some variables are created. Below the table with the available variables:


|Name    |Description    |Command(s) |Since|
|--------|---------------|-----------|-----|
|$parent|Get the parent context from a sub-query.|[SELECT](SQL-Query.md) and [TRAVERSE](SQL-Traverse.md)||
|$current|Current record to use in sub-queries to refer from the parent's variable|[SELECT](SQL-Query.md) and [TRAVERSE](SQL-Traverse.md)||
|$depth|The current depth of nesting|[TRAVERSE](SQL-Traverse.md)||
|$path|The string representation of the current path. Example:  #6-0.in.#5-0 |[TRAVERSE](SQL-Traverse.md)||
|$stack|The List of operation in the stack. Use it to access to the history of the traversal|[TRAVERSE](SQL-Traverse.md)||
|$history|The set of all the records traversed as a Set&lt;EntityId&gt;|[TRAVERSE](SQL-Traverse.md)||


To set custom variable use the [LET](SQL-Query.md#let-block) keyword.
