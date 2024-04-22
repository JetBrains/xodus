/*
 * Copyright ${inceptionYear} - ${year} ${owner}
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.entitystore.orientdb.query

import jetbrains.exodus.entitystore.orientdb.OEntityId


// Where
sealed interface OCondition : OQuery

// Property
class OEqualCondition(
    val field: String,
    val value: Any,
) : OCondition {

    override fun sql() = "$field = ?"
    override fun params() = listOf(value)
}

class OContainsCondition(
    val field: String,
    val value: String,
) : OCondition {

    override fun sql() = "$field containsText ?"
    override fun params() = listOf(value)
}

class OStartsWithCondition(
    val field: String,
    val value: String,
) : OCondition {

    override fun sql() = "$field like ?"
    override fun params() = listOf("${value}%")
}

// Binary
sealed class OBiCondition(
    val operation: String,
    val left: OCondition,
    val right: OCondition
) : OCondition {

    override fun sql() = "(${left.sql()} $operation ${right.sql()})"
    override fun params() = left.params() + right.params()
}

class OAndCondition(left: OCondition, right: OCondition) : OBiCondition("AND", left, right)
class OOrCondition(left: OCondition, right: OCondition) : OBiCondition("OR", left, right)
class ORangeCondition(
    val field: String,
    val minInclusive: Any,
    val maxInclusive: Any
) : OCondition {

    // https://orientdb.com/docs/3.2.x/sql/SQL-Where.html#between
    override fun sql() = "($field between ? and ?)"
    override fun params() = listOf(minInclusive, maxInclusive)
}

class OEdgeExistsCondition(
    val edge: String
) : OCondition {

    override fun sql() = "outE('$edge').size() > 0"
}

class OFieldExistsCondition(
    val field: String
) : OCondition {

    override fun sql() = "not($field is null)"
}

//link

class OLinkEqualCondition(
    val field: String,
    val valueId: OEntityId,
) : OCondition {

    override fun sql() = "out('$field').@rid = ?"
    override fun params() = listOf(valueId.asOId())
}
