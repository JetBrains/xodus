# SQL - `FIND REFERENCES`

Searches entities in the database that contain links to the given entity id in the database or a subset of the specified entitty type,
returning the matching entity ids.

**Syntax**

```sql
FIND REFERENCES <entity-id>|(<sub-query>) [entity-type-list]
```

- **`<entity-id>`** Defines the entity id you want to find links to in the database.
- **`<sub-query>`** Defines a sub-query for the entitty id's you want to find links to in the database.
- **`<types-list>`** Defines a comma-separated list of entitty types that you want to search.

This command returns a record containing two fields:

| Field | Description |
|---|---|
| `eid` | Entity id searched. |
| `referredBy` | Set of entity ids referenced by the entity id searched, if any.  In the event that no records reference the searched entity id, it returns an empty set. |


**Examples**

- Find entities that contain a link to `#5-0`:

  <pre>
    <code class="lang-sql userinput">FIND REFERENCES 5-0</code>

  RESULT:
  ------+-----------------
   eid  | referredBy      
  ------+-----------------
   #5-0 | [#10-23, #30-4] 
  ------+-----------------
  </pre>

- Find references to the person entity types

  <pre>
    <code class='lang-sql userinput'>FIND REFERENCES (SELECT FROM Person)</code>
  </pre>

- Find all entities of type `Profile` and `AnimalType` that contain a link to `#5-0`:

  <pre>
    <code class="lang-sql userinput"> FIND REFERENCES 5-0 [Profile, AnimalType]</code>
  </pre>


>For more information, see
>- [SQL Commands](SQL-Commands.md)
