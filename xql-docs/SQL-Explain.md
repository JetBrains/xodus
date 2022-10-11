# SQL - `EXPLAIN`

EXPLAIN SQL command returns information about query execution planning of a specific statement, without executing the statement itself.

**Syntax**

```
EXPLAIN <command>
```

- **`<command>`** Defines the command that you want to profile, eg. a SELECT statement

**Examples**


- Profile a query that executes on an entity type filtering based on an attribute:

  <pre>
  <code class='lang-sql userinput'>explain select from e where name = 'a'</code>

  Profiled command '[{
  executionPlanAsString:

  + FETCH FROM TYPE e
  + FILTER ITEMS WHERE 
    name = 'a'
  
  }]'

  </pre>

>For more information, see
>- [SQL Commands](SQL-Commands.md)
>- [PROFILE](SQL-Profile.md)
