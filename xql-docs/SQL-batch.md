
# SQL Batch

Xodus supports a minimal SQL engine to allow a batch of commands.
Batch of commands are very useful when you have to execute multiple things at once.

SQL Batch supports all the Xodus [SQL Commands](SQL-Commands.md), plus the following:
- ```begin```
- ```commit [retry <retry>]```, where:
 - `<retry>` is the number of retries in case of transaction rollback
- ```let <variable> = <SQL>```, to assign the result of a SQL command to a variable. To reuse the variable prefix it with the dollar sign $
- ```IF(<condition>){ <statememt>; [<statement>;]* }```. Look at [Conditional execution](SQL-batch.md#conditional-execution).
- ```WHILE(<condition>){ <statememt>; [<statement>;]* }```. Look at [Conditional execution](SQL-batch.md#loops).
- ```FOREACH(<variable> IN <expression>){ <statememt>; [<statement>;]* }```. Look at [Conditional execution](SQL-batch.md#loops).
- ```SLEEP <ms>```, put the batch in wait for `<ms>` milliseconds.
- ```console.log <text>```, logs a message in the console. Context variables can be used with `${<variable>}`.
- ```console.error <text>```, writes a message in the console's standard error. Context variables can be used with `${<variable>}`. 
- ```console.output <text>```, writes a message in the console's standard output. Context variables can be used with `${<variable>}`.
- ```return``` <value>, where value can be:
 - any value. Example: ```return 3```
 - any variable with $ as prefix. Example: ```return $a```
 - a query. Example: ```return (SELECT FROM Foo)```  

## Transaction

Example to create a new entity and connect it to an existent entity by creating a new link between them. 
If rollback occurs, repeat the transaction up to 100 times:

```sql
begin;
let account = insert into Account set name = 'Luke';
let city = select from City where name = 'London';
let e = create link Lives from $account to $city;
commit retry 100;
return $city;
```

Note the usage of $account and $city in further SQL commands.

## Conditional execution 
SQL Batch provides IF constructor to allow conditional execution.
The syntax is

```sql
if(<sql-predicate>){
   <statement>;
   <statement>;
   ...
}
```
`<sql-predicate>` is any valid SQL predicate (any condition that can be used in a WHERE clause).
In current release it's mandatory to have `IF(){`, `<statement>` and `}` on separate lines.

The right syntax is following:
```sql
if($a.size() > 0) { 
   ROLLBACK;
}
```

## Loops

SQL Batch provides two different loop blocks: FOREACH and WHILE

#### FOREACH
Loops on all the items of a collection and, for each of them, executes a set of SQL statements

The syntax is

```sql
FOREACH(<variable> IN <expression>){
   <statement>;
   <statement>;
   ...
}
```
Example
```sql
FOREACH ($i IN [1, 2, 3]){
  INSERT INTO Foo SET value = $i;
}
```


#### WHILE
Loops while a condition is true

The syntax is

```sql
WHILE(<condition>){
   <statement>;
   <statement>;
   ...
}
```

Example
```sql
LET $i = 0;
WHILE ($i < 10){
  INSERT INTO Foo SET value = $i;
  LET $i = $i + 1;
}
```
