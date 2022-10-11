# SQL - `PROFILE`

PROFILE SQL command returns information about query execution planning and statistics for a specific statement.
The statement is actually executed to provide the execution stats.

The result is the execution plan of the query (like for [EXPLAIN](SQL-Explain.md) ) with additional information about execution time spent on each step,
in microseconds.

**Syntax**

```
PROFILE <command>
```

- **`<command>`** Defines the command that you want to profile, eg. a SELECT statement

**Examples**


```sql
PROFILE SELECT sum(amount), orderDate 
FROM Orders 
WHERE orderDate > date("2012-12-09", "yyyy-MM-dd") 
GROUP BY orderDate  
```
result:

```
+ FETCH FROM INDEX Orders.orderDate (1.445μs)
  orderDate > date("2012-12-09", "yyyy-MM-dd")
+ EXTRACT VALUE FROM INDEX ENTRY
+ CALCULATE PROJECTIONS (5.065μs)
  amount AS _$$$OALIAS$$_1, orderDate
+ CALCULATE AGGREGATE PROJECTIONS (3.182μs)
  sum(_$$$OALIAS$$_1) AS _$$$OALIAS$$_0, orderDate
  GROUP BY OrderDate
+ CALCULATE PROJECTIONS (1.116μs)
  _$$$OALIAS$$_0 AS `sum(amount)`, orderDate
```

You can see the `(1.445μs)` at the end of the first line, it means that fetching from index `Orders.orderDate` took 1.445 microseconds (1.4 milliseconds)

>For more information, see
>- [SQL Commands](SQL-Commands.md)
>- [EXPLAIN](SQL-Explain.md)
