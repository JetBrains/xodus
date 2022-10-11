# SQL - `CREATE LINK`

Creates a new link between entities in the database.

**Syntax**

```sql
CREATE LINK <name> [UPSERT] FROM <eid>|(<query>)|[<eid>]* TO <eid>|(<query>)|[<eid>]*
                    [RETRY <retry> [WAIT <pauseBetweenRetriesInMs]] [BATCH <batch-size>]
```

- **`<name>`** Defines the name of link
- **`UPSERT`** Allows to skip the creation of links that already exist between two entities (ie. a unique link for a couple of entities). 
- **`RETRY`** Define the number of retries to attempt in the event of conflict.
- **`WAIT`** Defines the time to delay between retries in milliseconds.
- **`BATCH`** Defines whether it breaks the command down into smaller blocks and the size of the batches. 
This helps to avoid memory issues when the number of entities is too high.  By default, it is set to `100`.

When no links are created Xodus returns an error.  
In such cases, if the source or target entities don't exist, it rolls back the transaction.


**Examples**

- Create a link with name 'lnk' between two entities:

  <pre>
    <code class="lang-sql userinput">CREATE LINK lnk FROM #10-3 TO #11-4</code>
  </pre>


- Create links with name `watched` between all action movies in the database and the user Alexander, using sub-queries:

  <pre>
    <code class="lang-sql userinput">CREATE LINK watched FROM (SELECT FROM account WHERE name = 'Alexander') TO 
            (SELECT FROM movies WHERE type.name = 'action')</code>
  </pre>
