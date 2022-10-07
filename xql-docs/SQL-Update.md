# SQL - `UPDATE`

Update one or more entities in the current database.  

**Syntax**:

```sql
UPDATE <entity type>|<entityID>
  [SET|REMOVE <property-name> = <property-value>[,]*]|[CONTENT|MERGE <JSON>]
  [UPSERT]
  [RETURN <returning> [<returning-expression>]]
  [WHERE <conditions>]
  [LIMIT <max-records>] [TIMEOUT <timeout>]
```

- **`SET`** Defines the properties to update.
- **`REMOVE`** Removes an item in collection and map fields.
- **`CONTENT`** Replaces the entity content with a JSON document.
- **`MERGE`** Merges the entity content with a JSON document.
- **`UPSERT`** Updates an entity if it exists or inserts a new entity if it doesn't. 
- This avoids the need to execute two commands, (one for each condition, inserting and updating).  

  `UPSERT` requires a [`WHERE`](SQL-Where.md) clause and an entity type target.
- **`RETURN`** Specifies an expression to return instead of the entity count and what to do with the result-set returned by the expression.  
- The available return operators are:
  - `COUNT` Returns the number of updated entities.  This is the default return operator.
  - `BEFORE` Returns the entities before the update.
  - `AFTER` Return the entities after the update.
- [`WHERE`](SQL-Where.md)
- `LIMIT` Defines the maximum number of entities to update.
- `TIMEOUT` Defines the time you want to allow the update run before it times out.

>**NOTE**: The entity id must have a `#` prefix.  For instance, `#12-3`.

**Examples**:

- Update to change the value of a property:

  <pre>
   <code class="lang-sql userinput">UPDATE Profile SET nick = 'Alex' WHERE nick IS NULL</code>
  </pre>

- Update to remove a property from all entities:

  <pre>
    <code class="lang-sql userinput">UPDATE Profile REMOVE nick</code>
  </pre>

- Update to remove a value from a collection, if you know the exact value that you want to remove:

  Remove an element from a collection of links:

  <pre>
    <code class="lang-sql userinput">UPDATE Account REMOVE address = #12-0</code>
  </pre>

  Remove an element from a list or set of strings:

  <pre>
    <code class="lang-sql userinput">UPDATE Account REMOVE addresses = 'Foo'</code>
  </pre>

- Update to remove a value, filtering on value attributes.

  Remove addresses based in the city of Rome:

  <pre>
    <code class="lang-sql userinput">UPDATE Account REMOVE addresses = addresses[city = 'Rome']</code>
  </pre>

- Update to remove a value, filtering based on position in the collection.

  <pre>
    <code class="lang-sql userinput">UPDATE Account REMOVE addresses = addresses[1]</code>
  </pre>

  This remove the second element from a list, (position numbers start from `0`, so `addresses[1]` is the second element).

- Update to remove a value from a map

  <pre>
    <code class="lang-sql userinput">UPDATE Account REMOVE addresses = 'Alexander'</code>
  </pre>

- Update the first twenty entities that satisfy a condition:

  <pre>
    <code class="lang-sql userinput">UPDATE Profile SET nick = 'Alex' WHERE nick IS NULL LIMIT 20</code>
  </pre>

- Update an entity or insert if it doesn't already exist:

  <pre>
    <code class="lang-sql userinput">UPDATE Profile SET nick = 'Alex' UPSERT WHERE nick = 'Alex'</code>
  </pre>


- Updates using the `RETURN` keyword:

  <pre>
  <code class="lang-sql userinput">UPDATE ♯7-0 SET gender='male' RETURN AFTER @eid</code>
  <code class="lang-sql userinput">UPDATE ♯7-0 SET gender='male' RETURN AFTER @this</code>
  <code class="lang-sql userinput">UPDATE ♯7-0 SET gender='male' RETURN AFTER $current.exclude(
            "really_big_field")</code>
  </pre>

For more information on SQL syntax, see [`SELECT`](SQL-Query.md).
