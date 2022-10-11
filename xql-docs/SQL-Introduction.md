# Introduction

When it comes to query languages, SQL is the most widely recognized standard. The majority of developers have experience and are comfortable with SQL. 
For this reason Xodus Entity Store uses SQL as its query language and adds some extensions to enable object traversal functionality. 
There are a few differences between the standard SQL syntax and that supported by Xodus, but for the most part, it should feel very natural. 
The differences are covered in the [Xodus SQL dialect](#xodus-sql-dialect) section of this page.

If you are looking for the most efficient way to traverse an object graph, we suggest to use the [SQL-Match](SQL-Match.md) instead.

Many SQL commands share the [WHERE condition](SQL-Where.md). Keywords in Xodus SQL are case insensitive. 
Field names, entity types names and values are case sensitive. In the following examples keywords are in uppercase but this is not strictly required.

If you are not yet familiar with SQL, we suggest you to get the course on [KhanAcademy](http://cs-blog.khanacademy.org/2015/05/just-released-full-introductory-sql.html).

For example, if you have an entity type `MyClass` with a field named `id`, then the following SQL statements are equivalent:

```sql
SELECT FROM MyClass WHERE id = 1
select from MyClass where id = 1
```

The following is NOT equivalent.  Notice that the field name 'ID' is not the same as 'id'.

```sql
SELECT FROM MyClass WHERE ID = 1
```

## Extra resources
- [SQL syntax](SQL-Syntax.md)
- [SQL projections](SQL-Projections.md)
- [SQL conditions](SQL-Where.md)
 - [Where clause](SQL-Where.md)
 - [Operators](SQL-Where.md#operators)
 - [Functions](SQL-Where.md#functions)
- [Pagination](Pagination.md)
- [SQL batch](SQL-batch.md)
- [SQL-Match](SQL-Match.md) for traversing graphs

## Xodus SQL dialect

Xodus supports SQL as a query language with some differences compared with SQL. It was decided to avoid creating Yet-Another-Query-Language. 
Instead we started from familiar SQL with extensions to work with object graph. We prefer to focus on standards.

If you want learn SQL, there are many online courses such as:
- [Online course Introduction to Databases by Jennifer Widom from Stanford university](https://www.coursera.org/course/db)
- [Introduction to SQL at W3 Schools](http://www.w3schools.com/sql/sql_intro.asp)
- [Beginner guide to SQL](https://blog.udemy.com/beginners-guide-to-sql/)
- [SQLCourse.com](http://www.sqlcourse2.com/intro2.html)
- [YouTube channel Basic SQL Training by Joey Blue](http://www.youtube.com/playlist?list=PLD20298E653A970F8)

To know more, look to [Xodus SQL Syntax](SQL-Syntax.md).

## No JOINs
The most important difference between Xodus and a Relational Database is that relationships are represented by `LINKS` instead of JOINs.

For this reason, the classic JOIN syntax is not supported. Xodus uses the "dot (`.`) notation" to navigate `LINKS`.\
Example 1 : In SQL you might create a join such as:
```sql
SELECT *
FROM Employee A, City B
WHERE A.city = B.id
AND B.name = 'Kyiv'
```
In Xodus, an equivalent operation would be:
```sql
SELECT * FROM Employee WHERE city.name = 'Kyiv'
```
This is much more straight forward and powerful! If you use multiple JOINs, the Xodus SQL equivalent will be an even larger benefit.\
Example 2:  In SQL you might create a join such as:
```sql
SELECT *
FROM Employee A, City B, Country C,
WHERE A.city = B.id
AND B.country = C.id
AND C.name = 'Ukraine'
```
In Xodus, an equivalent operation would be:
```sql
SELECT * FROM Employee WHERE city.country.name = 'Ukraine'
```

## Projections
In SQL, projections are mandatory and you can use the star character `*` to include all of the fields.
With Xodus this type of projection is optional. Example: In SQL to select all of the columns of Customer you would write:
```sql
SELECT * FROM Customer
```
In Xodus, the `*` is optional:
```sql
SELECT FROM Customer
```

See [SQL projections](SQL-Projections.md)

## DISTINCT

In Xodus you can use DISTINCT keyword exactly as in a relational database:
```sql
SELECT DISTINCT name FROM City
```

## HAVING

Xodus does not support the `HAVING` keyword, but with a nested query it's easy to obtain the same result. Example in SQL:
```SQL
SELECT city, sum(salary) AS salary
FROM Employee
GROUP BY city
HAVING salary > 1000
```

This groups all of the salaries by city and extracts the result of aggregates with the total salary greater than 1,000 dollars.
In Xodus the `HAVING` conditions go in a select statement in the predicate:

```SQL
SELECT FROM ( SELECT city, SUM(salary) AS salary FROM Employee GROUP BY city ) WHERE salary > 1000
```

## Select from multiple targets

Xodus allows only one entity type (class if you wish) (entity types are equivalent to tables in this discussion) as opposed to SQL,
which allows for many tables as the target.  If you want to select from 2 entity types, you have to execute 2 sub queries and join them with the `UNIONALL` function:
```sql
SELECT FROM C1, C2
```
In Xodus, you can accomplish this with a few variable definitions and by using the `expand` function to the union:
```sql
SELECT EXPAND( $c ) LET $a = ( SELECT FROM C1 ), $b = ( SELECT FROM C2 ), $c = UNIONALL( $a, $b )
```
