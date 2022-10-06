
# SQL - `INSERT`

The [`INSERT`](SQL-Insert.md) command adds new enitity in the database.
**Syntax**:

```sql
INSERT INTO <entity type>
  [(<property>[,]*) VALUES (<expression>[,]*)[,]*]|
  [SET <property> = <expression>|<sub-command>[,]*]|
  [CONTENT {<JSON>}]
  [RETURN <expression>] 
  [FROM <query>]
```

- **`CONTENT`** Defines JSON data as an option to set property values.
- **`RETURN`** Defines an expression to return instead of the number of inserted entities.  You can use any valid SQL expression.  The most common use-cases,
  - `@eid` Returns the ID of the new entity.
  - `@this` Returns the entire new entity.
- **`FROM`** Fetches what you want to insert in the result-set.

**Examples**:

- Inserts a new entity with the name `Jay` and surname `Miner`.

  As an example, in the SQL-92 standard, such as with a Relational database, you might use:

  <pre>
    <code class="lang-sql userinput">INSERT INTO Profile (name, surname) 
            VALUES ('Jay', 'Miner')</code>
  </pre>

  Alternatively, in the Xodus abbreviated syntax, the query would be written as,

  <pre>
     <code class="lang-sql userinput">INSERT INTO Profile SET name = 'Jay', surname = 'Miner'</code>
  </pre>

  In JSON content syntax, it would be written as this,

  <pre>
    <code class="lang-sql userinput">INSERT INTO Profile CONTENT {"name": "Jay", "surname": "Miner"}</code>
  </pre>


- Insert several entries at the same time:

  <pre>
    <code class="lang-sql userinput">INSERT INTO Profile(name, surname) VALUES ('Jay', 'Miner'), 
            ('Frank', 'Hermier'), ('Emily', 'Sout')</code>
  </pre>

- Insert a new entry, adding a relationship.

  In SQL-93 syntax:

  <pre>
    <code class="lang-sql userinput">INSERT INTO Employee (name, boss) VALUES ('jack', #11-09)</code>
  </pre>

  In the Xodus abbreviated syntax:

  <pre>
    <code class="lang-sql userinput">INSERT INTO Employee SET name = 'jack', boss = #11-99</code>
  </pre>

- Insert a new entry, add a collection of relationships.

  In SQL-93 syntax:

  <pre>
    <code class="lang-sql userinput">INSERT INTO Profile (name, friends) VALUES ('Alexandr', [#10-3, #10-4])</code>
  </pre>

  In the Xodus abbreviated syntax:

  <pre>
    <code class="lang-sql userinput">INSERT INTO Profiles SET name = 'Alexander', friends = [#10-3, #10-4]</code>
  </pre>

- Inserts using [`SELECT`](SQL-Query.md) sub-queries

  <pre>
    <code class="lang-sql userinput">INSERT INTO Diver SET name = 'Alexander', buddy = (SELECT FROM Diver 
            WHERE name = 'Marko')</code>
  </pre>

- Inserts using [`INSERT`](SQL-Insert.md) sub-queries:

  <pre>
    <code class="lang-sql userinput">INSERT INTO Diver SET name = 'Alexander', buddy = (INSERT INTO Diver 
            SET name = 'Marko')</code>
  </pre>

- Insert from a query.

  To copy records from another type, use:

  <pre>
    <code class="lang-sql userinput">INSERT INTO GermanyClient FROM SELECT FROM Client WHERE 
            country = 'Germany'</code>
  </pre>

  This inserts all entries from the type `Client` where the country is Germany, in the type `GermanyClient`.

  To copy records from one type into another, while adding a property:

  <pre>
    <code class="lang-sql userinput">INSERT INTO GermanyClient FROM SELECT *, true AS copied FROM Client 
            WHERE country = 'Germany'</code>
  </pre>

  This inserts all records from type `Client` where the country is Germany into the type `GermanClient`, with the additional property `copied` to the value `true`.

For more information on SQL, see [SQL Commands](SQL-Commands.md).
