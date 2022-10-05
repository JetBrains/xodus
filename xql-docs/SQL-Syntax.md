# Xodus SQL syntax

Xodus Query Language is and SQL dialect.

This page lists all the details about its syntax.

## Identifiers
An identifier is a name that identifies an entity. Identifiers can refer to
- entity type names
- property names
- aliases
- function names
- method names
- named parameters
- variable names (LET)

An identifier is a sequence of characters delimited by back-ticks ``` ` ```. 
Examples of valid identifiers are
- ``` `surname` ```
- ``` `name and surname` ```
- ``` `foo.bar` ```
- ``` `a + b` ```
- ``` `select` ```

The back-tick character can be used as a valid character for identifiers, but it has to be escaped with a backslash, eg.
- ``` `foo \` bar` ```

The following are reserved identifiers, they can NEVER be used with a different meaning (upper or lower case):

- ``` @eid ```: entity ID
- ``` @type ```: entity type 
- ``` @fieldTypes ```: entity field types (reserved for future usage)

**Simplified identifiers**

Identifiers that start with a letter or with `$` and that contain only numbers, letters and underscores, can be written without back-tick quoting. 
Reserved words cannot be used as simplified identifiers. Valid simplified identifiers are
- ```name```
- ```name_and_surname```
- ```$foo```
- ```name_12```


Examples of INVALID queries for wrong identifier syntax

```SQL
/* INVALID - `from` is a reserved keyword */
SELECT from from from 
/* CORRECT */
SELECT `from` from `from` 

/* INVALID - simplified identifiers cannot start with a number */
SELECT name as 1name from Foo
/* CORRECT */
SELECT name as `1name` from Foo

/* INVALID - simplified identifiers cannot contain `-` character, `and` is a reserved keyword */
SELECT name-and-surname from Foo
/* CORRECT 1 - `name-and-surname` is a single field name */
SELECT `name-and-surname` from Foo
/* CORRECT 2 - `name`, `and` and `surname` are numbers and the result is the subtraction */
SELECT name-`and`-surname from Foo
/* CORRECT 2 - with spaces  */
SELECT name - `and` - surname from Foo

/* INVALID - wrong back-tick escaping */
SELECT `foo`bar` from Foo
/* CORRECT */
SELECT `foo\`bar` from Foo

```
**Case sensitivity**

In current version all the identifiers are case sensitive.

## Reserved words

In Xodus SQL the following are reserved words

- AFTER
- AND
- AS
- ASC
- BATCH
- BEFORE
- BETWEEN
- BREADTH_FIRST
- BY
- CONTAINS
- CONTAINSALL
- CONTAINSKEY
- CONTAINSTEXT
- CONTAINSVALUE
- CREATE
- DEFAULT
- DEFINED
- DELETE
- DEPTH_FIRST
- DESC
- DISTINCT
- FETCHPLAN
- FROM
- INCREMENT
- INSERT
- INSTANCEOF
- INTO
- IS
- LET
- LIKE
- LIMIT
- LOCK
- MATCH
- MATCHES
- MAXDEPTH
- NOCACHE
- NOT
- NULL
- OR
- PARALLEL
- POLYMORPHIC
- RETRY
- RETURN
- SELECT
- SKIP
- STRATEGY
- TIMEOUT
- TRAVERSE
- UNSAFE
- UNWIND
- UPDATE
- UPSERT
- WAIT
- WHERE
- WHILE

## Base types

Accepted base types in Xodus SQL are:
- **integer numbers**: 

Valid integers are
```
(32bit)
1
12345678
-45

(64bit)
1L
12345678L
-45L
```

- **floating point numbers**: single or double precision

Valid floating point numbers are:
```
(single precision)
1.5
12345678.65432
-45.0

(double precision)
0.23D
.23D
```

- **strings**: delimited by `'` or by `"`. Single quotes, double quotes and back-slash inside strings can escaped using a back-slash

Valid strings are:
```
"foo bar"
'foo bar'
"foo \" bar"
'foo \' bar'
'foo \\ bar'
```

- **booleans**: boolean values are case sensitive

Valid boolean values are
```
true
false
```

Boolean value constants are case insensitive, so also `TRUE`, `True` and so on are valid.


- **links**: A link is a pointer to an entity in the database

In SQL a link is represented as follows (short and extended notation):

```
#<entity-type-id>-<entity-instance-id>

or

{"@eid": "#<entity-type-id>-<entity-instance-id>"}
```
eg.
```
#12-15

or

{"@eid": "#12-15"}
```

The bracket notation is mandatory inside JSON, as the short notation is not a valid value in JSON.

- **null**: case insensitive (for consistency with IS NULL and IS NOT NULL conditions, that are case insensitive)

Valid null expressions include
```
NULL
null
Null
nUll
...
```

## Numbers

Xodus can store five different types of numbers
- Integer: 32bit signed
- Long: 64bit signed
- Float: decimal 32bit signed
- Duoble: decimal 64bit signed

**Integers** are represented in SQL as plain numbers, eg. `123`. 
If the number represented exceeds the Integer maximum size (see Java java.lang.Integer `MAX_VALUE` and `MIN_VALUE`),
then it's automatically converted to a Long. 

**Longs** are represented in SQL as numbers with `L` suffix, eg. `123L` (L can be uppercase or lowercase).
Plain numbers (withot L prefix) that exceed the Integer range are also automatically converted to Long.
If the number represented exceeds the Long maximum size (see Java java.lang.Long `MAX_VALUE` and `MIN_VALUE`), then the result is `NULL`;

Integer and Long numbers can be represented in base 10 (decimal), 8 (octal) or 16 (hexadecimal):
- decimal: `["-"] ("0" | ( ("1"-"9") ("0"-"9")* ) ["l"|"L"]`, eg. 
  - `15`, `15L`  
  - `-164` 
  - `999999999999`
- octal: `["-"] "0" ("0"-"7")+ ["l"|"L"]`, eg. 
  - `01`, `01L` (equivalent to decimal 1) 
  - `010`, `010L` (equivalent to decimal 8)
  - `-065`, `-065L` (equivalent to decimal 53)
- hexadecimal: `["-"] "0" ("x"|"X") ("0"-"9"," a"-"f", "A"-"F")+ ["l"|"L"]`, eg.
  - `0x1`, `0X1`, `0x1L` (equivalent to 1 decimal)
  - `0x10` (equivalent to decimal 16)
  - `0xff`, `0xFF` (equivalent to decimal 255)
  - `-0xff`, `-0xFF` (equivalent to decimal -255)
  
**Float** numbers are represented in SQL as `[-][<number>].<number>`, eg. valid Float values are `1.5`, `-1567.0`, `.556767`. 
If the number represented exceeds the Float maximum size (see Java java.lang.Float `MAX_VALUE` and `MIN_VALUE`), 
then it's automatically converted to a Double. 

**Double** numbers are represented in SQL as `[-][<number>].<number>D` (D can be uppercase or lowercase),
eg. valid Float values are `1.5d`, `-1567.0D`, `.556767D`. 
If the number represented exceeds the Double maximum size (see Java java.lang.Double `MAX_VALUE` and `MIN_VALUE`), then the result is `NULL`


Float and Double numbers can be represented as decimal, decimal with exponent, hexadecimal and hexadecimal with exponent.
Here is the full syntax:

```

FLOATING_POINT_LITERAL: ["-"] ( <DECIMAL_FLOATING_POINT_LITERAL> | <HEXADECIMAL_FLOATING_POINT_LITERAL> )

DECIMAL_FLOATING_POINT_LITERAL:
      (["0"-"9"])+ "." (["0"-"9"])* (<DECIMAL_EXPONENT>)? (["f","F","d","D"])?
      | "." (["0"-"9"])+ (<DECIMAL_EXPONENT>)? (["f","F","d","D"])?
      | (["0"-"9"])+ <DECIMAL_EXPONENT> (["f","F","d","D"])?
      | (["0"-"9"])+ (<DECIMAL_EXPONENT>)? ["f","F","d","D"]
  >

DECIMAL_EXPONENT: ["e","E"] (["+","-"])? (["0"-"9"])+ 

HEXADECIMAL_FLOATING_POINT_LITERAL:
        "0" ["x", "X"] (["0"-"9","a"-"f","A"-"F"])+ (".")? <HEXADECIMAL_EXPONENT> (["f","F","d","D"])?
      | "0" ["x", "X"] (["0"-"9","a"-"f","A"-"F"])* "." (["0"-"9","a"-"f","A"-"F"])+ <HEXADECIMAL_EXPONENT> (["f","F","d","D"])?

HEXADECIMAL_EXPONENT: ["p","P"] (["+","-"])? (["0"-"9"])+ 
```

Eg. 
- base 10 
  - `0.5` 
  - `0.5f`, `0.5F`, `2f` (ATTENTION, this is NOT hexadecimal)
  - `0.5d`, `0.5D`, `2D` (ATTENTION, this is NOT hexadecimal)
  - `3.21e2d` equivalent to `3.21 * 10^2 = 321`
- base 16
  - `0x3p4d` equivalent to `3 * 2^4 = 48`  
  - `0x3.5p4d` equivalent to `3.5(base 16) * 2^4`


#### Mathematical operations

Mathematical Operations with numbers follow these rules:
- Operations are calculated from left to right, following the operand priority. 
- When an operation involves two numbers of different type, both are converted to the higher precision type between the two. 

Eg. 

```
15 + 20L = 15L + 20L     // the 15 is converted to 15L

15L + 20 = 15L + 20L     // the 20 is converted to 20L

15 + 20.3 = 15.0 + 20.3     // the 15 is converted to 15.0

15.0 + 20.3D = 15.0D + 20.3D     // the 15.0 is converted to 15.0D
```

the overflow follows Java rules.


## Collections

Xodus supports two types of collections:
- **Lists**: ordered, allow duplicates
- **Sets**: no duplicates
 
The SQL notation allows to create `Lists` with square bracket notation, eg.
```
[1, 3, 2, 2, 4]
```

A `List` can be converted to a `Set` using the `.asSet()` method:

```
[1, 3, 2, 2, 4].asSet() = [1, 3, 2, 4] /*  the order of the elements in the resulting set is not guaranteed */
```

## Binary data
Xodus can store binary data (byte arrays) in document fields. 
There is no native representation of binary data in SQL syntax, insert/update a binary field you have to use `decode(<base64string>, "base64")` function.

To obtain the base64 string representation of a byte array, you can use the function `encode(<byteArray>, "base64")`

## Expressions

Expressions can be used as:

- single projections
- operands in a condition
- items in a GROUP BY 
- items in an ORDER BY
- right argument of a LET assignment

Valid expressions are:
- `<base type value>` (string, number, boolean)
- `<field name>`
- `<@attribute name>`
- `<function invocation>`
- `<expression> <binary operator> <expression>`: for operator precedence, see below table.
- `<unary operator> <expression>` 
- `( <expression> )`: expression between parenthesis, for precedences
- `( <query> )`: query between parenthesis
- `[ <expression> (, <expression>)* ]`: a list, an ordered collection that allows duplicates, eg. `["a", "b", "c"]`)
- `{ <expression>: <expression> (, <expression>: <expression>)* }`: the result is an entity, with <field>:<value> values, eg. `{"a":1, "b": 1+2+3, "c": foo.bar.size() }`. 
  The key name is converted to String if it's not.
- `<expression> <modifier> ( <modifier> )*`: a chain of modifiers (see below)
- `<json>`: It is translated to an entity. Nested JSON is allowed and is translated to nested entities
- `<expression> IS NULL`: check for null value of an expression
- `<expression> IS NOT NULL`: check for non null value of an expression

### Modifiers

A modifier can be
- a dot-separated field chain, eg. `foo.bar`. Dot notation is used to navigate relationships in entity fields. eg.
  ```
  john = {
            name: "John",
            surname: "Jones",
            address: {
               city: {
                  name: "London"
               }
            }
         }
            
  john.address.city.name = "London"
  ```
  
- a method invocation, eg. `foo.size()`.

  Method invocations can be chained, eg. `foo.toLowerCase().substring(2, 4)`
  
- a square bracket filter, eg. `foo[1]` or `foo[name = 'John']`


### Square bracket filters

Square brackets can be used to filter collections or maps. 

`field[ ( <expression> | <range> | <condition> ) ]`

Based on what is between brackets, the square bracket filtering has different effects:

- `<expression>`: If the expression returns an Integer or Long value (i), the result of the square bracket filtering
is the i-th element of the collection/map. If the result of the expresson (K) is not a number, 
  the filtering returns the value corresponding to the key K in the map field. 
  If the field is not a collection/map, the square bracket filtering returns `null`.
The result of this filtering is ALWAYS a single value.
- `<range>`: A range is something like `M..N`  or `M...N` where M and N are integer/long numbers, eg. `fieldName[2..5]`. 
  The result of range filtering is a collection that is a subet of the original field value, containing all the items from position M (included) 
  to position N (excluded for `..`, included for `...`).
  Eg. if `fieldName = ['a', 'b', 'c', 'd', 'e']`, `fieldName[1..3] = ['b', 'c']`, `fieldName[1...3] = ['b', 'c', 'd']`.
  Ranges start from `0`. The result of this filtering is ALWAYS a list (ordered collection, allowing duplicates).
  If the original collection was ordered, then the result will preserve the order.
- `<condition>`: A normal SQL condition, that is applied to each element in the `objects` collection. 
  The result is a sub-collection that contains only items that match the condition. 
  Eg. `objects = [{foo = 1},{foo = 2},{foo = 5},{foo = 8}]`, `objects[foo > 4] = [{foo = 5},{foo = 8}]`.
  The result of this filtering is ALWAYS a list (ordered collection, allowing duplicates). 
  If the original collection was ordered, then the result will preserve the order.

## Conditions

A condition is an expression that returns a boolean value.

An expression that returns something different from a boolean value is always evaluated to `false`.

## Comparison Operators

- **`=`  (equals)**: If used in an expression, it is the boolean equals (eg. `select from Foo where name = 'John'`. 
  If used in an SET section of INSERT/UPDATE statements or on a LET statement, it represents a variable assignment (eg. `insert into Foo set name = 'John'`)
- **`!=` (not equals)**: inequality operator. 
- **`<>` (not equals)**: same as `!=`
- **`>`  (greater than)**
- **`>=` (greater or equal)**
- **`<`  (less than)**
- **`<=` (less or equal)**

## Math Operators

- **`+`  (plus)**: addition if both operands are numbers, string concatenation (with string conversion) if one of the operands is not a number.
  The order of calculation (and conversion) is from left to right, eg `'a' + 1 + 2 = 'a12'`, `1 + 2 + 'a' = '3a'`. 
  It can also be used as a unary operator (no effect)
- **`-`  (minus**): subtraction between numbers. Non-number operands are evaluated to zero. Null values are treated as a zero, eg `1 + null = 1`. 
  Minus can also be used as a unary operator, to invert the sign of a number
- **`*`  (multiplication)**: multiplication between numbers. If one of the operands is null, the multiplication will evaluate to null. 
- **`/`  (division)**: division between numbers. If one of the operands is null, the division will evaluate to null.. The result of a division by zero is NaN
- **`%`  (modulo)**: modulo between numbers. If one of the operands is null, the modulo will evaluate to null.. 
- **`>>`  (bitwise right shift)**: shifts bits on the right operand by a number of positions equal to the right operand. Eg. `8 >> 2 = 2`.
  Both operands have to be Integer or Long values, otherwise the result will be null.  
- **`>>>`  (unsigned bitwise right shift)** The same as `>>`, but with negative numbers it will fill with `1` on the left.
  Both operands have to be Integer or Long values, otherwise the result will be null.
- **`<<`  (bitwise right shift)** shifts bits on the left, eg. `2 << 2 = 8`. Both operands have to be Integer or Long values, otherwise the result will be null.
- **`&`  (bitwise AND)** executes a bitwise AND operation. Both operands have to be Integer or Long values, otherwise the result will be null.
- **`|`  (bitwise OR)** executes a bitwise OR operation. Both operands have to be Integer or Long values, otherwise the result will be null.
- **`^`  (bitwise XOR)** executes a bitwise XOR operation. Both operands have to be Integer or Long values, otherwise the result will be null.
- **`||`**: array concatenation (see below for details)

### Math Operators precedence


| type                  |   Operators     |
|-----------------------|-----------------|
| multiplicative        | `*` `/` `%`     |
| additive	            |   `+` `-`       |
| shift	                | `<<` `>>` `>>>` |
| bitwise AND	        |   `&`           |
| bitwise exclusive OR	|  `^`            |
| bitwise inclusive OR	|   <code>&#124;</code>        |
| array concatenation	|   <code>&#124;&#124;</code>        |

## Math + Assign operators

These operators can be used in UPDATE statements to update and set values. The semantics is the same as the operation plus the assignment,
eg. `a += 2` is just a shortcut for `a = a + 2`.

- **`+=`  (add and assign)**: adds right operand to left operand and assigns the value to the left operand. Returns the final value of the left operand. 
  If one of the operands is not a number, then this operator acts as a `concatenate string values and assign`
- **`-=`  (subtract and assign)**: subtracts right operand from left operand and assigns the value to the left operand. 
  Returns the final value of the left operand
- **`*=`  (multiply and assign)**: multiplies left operand and right operand and assigns the value to the left operand. 
  Returns the final value of the left operand
- **`/=`  (divide and assign)**: divides left operand by right operand and assigns the value to the left operand.
  Returns the final value of the left operand
- **`%=`  (modulo and assign)**: calculates left operand modulo right operand and assigns the value to the left operand. 
  Returns the final value of the left operand

## Array concatenation

The `||` operator concatenates two arrays.

```
[1, 2, 3] || [4, 5] = [1, 2, 3, 4, 5]
```

If one of the elements is not an array, then it's converted to an array of one element, before the concatenation operation is executed

```
[1, 2, 3] || 4 = [1, 2, 3, 4]

1 || [2, 3, 4] = [1, 2, 3, 4]

1 || 2 || 3 || 4 = [1, 2, 3, 4]
```

To add an array, you have to wrap the array element in another array:

```
[[1, 2], [3, 4]] || [5, 6] = [[1, 2], [3, 4], 5, 6]

[[1, 2], [3, 4]] || [[5, 6]] = [[1, 2], [3, 4], [5, 6]]
```

The result of an array concatenation is always a List (ordered and with duplicates).
  The order of the elements in the list is the same as the order in the elements in the source arrays, in the order they appear in the original expression.

To transform the result of an array concatenation in a Set (remove duplicates), just use the `.asSet()` method

```
[1, 2] || [2, 3] = [1, 2, 2, 3]

([1, 2] || [2, 3]).asSet() = [1, 2, 3] 
```

**Specific behavior of NULL**

Null value has no effect when applied to a || operation. eg.

```
[1, 2] || null = [1, 2]

null || [1, 2] = [1, 2]
```

To add null values to a collection, you have to explicitly wrap them in another collection, eg.

```
[1, 2] || [null] = [1, 2, null]
```



## Boolean Operators

- **`AND`**: logical AND
- **`OR`**: logical OR
- **`NOT`**: logical NOT
- **`CONTAINS`**: checks if the left collection contains the right element. The left argument has to be a colleciton, otherwise it returns FALSE.
  It's NOT the check of colleciton intersections, so `['a', 'b', 'c'] CONTAINS ['a', 'b']` will return FALSE, while `['a', 'b', 'c'] CONTAINS 'a'` will return TRUE. 
- **`IN`**: the same as CONTAINS, but with inverted operands.
- **`CONTAINSKEY`**: for maps, the same as for CONTAINS, but checks on the map keys
- **`CONTAINSVALUE`**: for maps, the same as for CONTAINS, but checks on the map values
- **`LIKE`**: for strings, checks if a string contains another string. `%` is used as a wildcard, eg. `'foobar LIKE '%ooba%''`
- **`IS DEFINED`** (unary): returns TRUE is a field is defined in an entity
- **`IS NOT DEFINED`** (unary): returns TRUE is a field is not defined in an entity
- **`BETWEEN - AND`** (ternary): returns TRUE is a value is between two values, eg. `5 BETWEEN 1 AND 10`
- **`MATCHES`**: checks if a string matches a regular expression
- **`INSTANCEOF`**: checks the type of a value, the right operand has to be the a String representing an entity type name, eg. `father INSTANCEOF 'Person'` 
