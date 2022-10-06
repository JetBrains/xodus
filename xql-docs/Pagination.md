# Pagination

There are 2 ways to achieve pagination:

## Use the SKIP-LIMIT

The first and simpler way to do pagination is to use the `SKIP`/`LIMIT` approach. 
This is the slower way because Xodus repeats the query and just skips the first X entities from the result.

Syntax:
```sql
SELECT FROM <target> [WHERE ...] SKIP <entities-to-skip> LIMIT <max-entites>
```
Where:
- **entities-to-skip** is the number of entites to skip before starting to collect them as the result set
- **max-entites** is the maximum number of entities returned by the query

Example
## Use the EntityID-LIMIT

This method is faster than the `SKIP`-`LIMIT` because Xodus will begin the scan from the starting EntityID. 
The downside is that it's more complex to use.

The trick here is to execute the query multiple times setting the `LIMIT` as the page size and using the greater than `>` operator against `@eid`. 
The **lower-rid** is the starting point to search, for example `#10-300`.

Syntax:
```sql
SELECT FROM <target> WHERE @eid > <lower-eid> ... [LIMIT <max-records>]
```

Where:
- **lower-eid** is the exclusive lower bound of the range as EntityID
- **max-entities** is the maximum number of entities returned by the query

In this way, Xodus will start to scan the cluster from the given position **lower-eid** + 1. 
After the first call, the **lower-eid** will be the id of the last entity returned by the previous call.
To scan entities from the beginning, use `#-1--1` as **lower-eid**.
