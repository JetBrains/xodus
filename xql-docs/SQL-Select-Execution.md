## SQL SELECT Statements Execution

The execution flow of a SELECT statement is made of many steps. 
Understanding these steps will help you to write better and more optimized queries.

### Overivew

The SELECT query execution, at a very high level, is made of three steps:
- Query optimization
- Creation of execution plans
- Choice of the optimal execution plan
- Actual execution

#### Query optimization

The first step for the query executor is to run a query optimizer. 
This operation can change the internal structure of the SQL statement to make it more efficient, preserving the same semantics of the original query.

Typical optimization steps are:

- Early calculation of expressions

eg. consider the following statement
```sql
SELECT FROM Person WHERE fullName = "John" + " " + "Smith" 
```
The result of the string concatenation `"John" + " " + "Smith"` does not depend on the query context (eg. the content of a record in the result set),
so it can be calculated only once in the execution phase. The result of the optimization of this query will be the equivalent of

```sql
SELECT FROM Person WHERE fullName = "John Smith" 
```

- Early calculation of sub-queries

eg. consider the following statement
```sql
SELECT FROM Person WHERE father in (SELECT FROM Person WHERE name = 'John')
```
The result of the subquery does not depend on the parent query context, so it can be executed only once,
and then use the result as an argument for the parent query:

```sql
LET $a = (SELECT FROM Person WHERE name = 'John');
SELECT FROM Person WHERE father in $a 
```

It is possible only if the subquery does not depend on the context of the parent query, so for example the following cannot be split:

```sql
SELECT FROM Person WHERE father in (SELECT FROM Person WHERE name = 'John' and surname = $parent.$current.surname)
```

- Refactoring of the WHERE conditions 

eg. consider the following:

```sql
SELECT FROM Person 
WHERE 
(name = 'John' AND surname = 'Smith') 
OR (name = 'John' AND surname = 'Doe') 
OR (name = 'John' AND surname = 'Travolta') 
OR (name = 'John' AND surname = 'Lennon')
OR (name = 'John' AND surname = 'Nash') 
```

If the WHERE condition is evaluated as is, the condition `name = 'John'`
has to be evaluated five times for each record that does not have a 'John' as a name. This query can be rewritten as:

```sql
SELECT FROM Person 
WHERE 
name = 'John' AND (
  surname = 'Smith'
  OR surname = 'Doe'
  OR surname = 'Travolta'
  OR surname = 'Lennon'
  OR surname = 'Nash'
)
```

#### Creation of execution plans

An execution plan is a sequence of operations that the query engine has to execute to calculate the query result.

Each step in the execution plan typically does a single operation, eg. fetch data of provided entity type, filter results, calculate projections and so on.

For the same query, Xodus can calculate multiple execution plans, based on involvement of indexes, optimized sorting and so on.

An execution plan has an execution cost that depends on the number of processed records, 
the number of operations performed and the elaboration time. 
The query executor uses the execution cost as the main criterion to choose the optimal execution plan.

#### Choice of the optimal execution plan

If the query executor produces multiple execution plans, then it has to choose the more convenient one to actually execute the query.
This choice is made based on the execution cost: the execution plan with the minimum cost is chosen.

#### Actual execution

After choosing the optimal execution plan, it is just executed.
The execution of an execution plan is just the execution of all the steps that it represents.

### Query Execution Plan

As described above, an execution plan is a sequence of steps that have to be executed to calculate a query result.

Different queries will have different execution plans.

The typical execution plan is made of the following steps:

- fetch from query target (that can be an entity type , enity id)
- evaluate LET expressions
- calculate query projections
- filter results
- aggregate data (eg. aggregate functions + GROUP BY)
- unwind projections
- sort result (ORDER BY)
- SKIP
- LIMIT

Obviously, a simple query like `SELECT FROM Person` will have a very simple execution plan made of a single step (the fetch of `Person` entitis), 
while a complex query will have an execution plan made of multiple steps

To display the execution plan of a query, without executing it, you can just execute the query prefixing it with `EXPLAIN`, eg.

```sql
EXPLAIN SELECT FROM Person 
```
