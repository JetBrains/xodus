# SQL - `TRUNCATE TYPE`

Deletes all entities of the provided type.  

**Syntax**

```
TRUNCATE TYPE <EnityType> [ POLYMORPHIC ] [ UNSAFE ] 
```

- **`<EntityType>`** Defines the entity type you want to truncate.
- **`POLYMORPHIC`** Defines whether the command also truncates the whole hierarchy of entity types (starting from currrent type as parent).
- **`UNSAFE`** Defines whether the command forces deletion of entites (does not take into account present incoming and outgoing links)

**Examples**

- Remove all entities of the type `Profile`:

  <pre>
    <code class='lang-sql userinput'>TRUNCATE TYPE Profile</code>
  </pre>

>For more information, see
>- [`DELETE`](SQL-Delete.md)
>- [SQL Commands](SQL-Commands.md)
