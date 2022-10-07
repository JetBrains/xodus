# SQL - `DELETE`

Removes one or more entities from the database. 
You can refine the set of entities that it removes using the [`WHERE`](SQL-Where.md) clause.

**Syntax:**

```sql
DELETE FROM <Entity Type>|<EntityId> [RETURN <returning>]
  [WHERE <Condition>*] [LIMIT <MaxEntities>] [TIMEOUT <timeout>]
```
- **`RETURN`** Defines  what values the database returns.  It takes one of the following values:
  - `COUNT` Returns the number of deleted entities.  This is the default option.
  - `BEFORE` Returns the number of entities before the removal.
- **[`WHERE`](SQL-Where.md)** Filters the entities you want to delete.
- **`LIMIT`** Defines the maximum number of entities to delete.
- **`TIMEOUT`** Defines the time period to allow the operation to run, before it times out.
- **`UNSAFE`** Allows for the processing of a DELETE on an entity without validation of presence of existing links to/from entity.
**Examples:**

- Delete all entitis with the surname `unknown`, ignoring case:

  <pre>
    <code class="lang-sql userinput">DELETE FROM Profile WHERE surname.toLowerCase() = 'unknown'</code>
  </pre>

For more information, see [SQL Commands](SQL-Commands.md).
