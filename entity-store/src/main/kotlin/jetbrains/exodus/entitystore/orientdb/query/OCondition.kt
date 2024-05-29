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


// Where
sealed interface OCondition : OQuery

// Property
class OEqualCondition(
    val field: String,
    val value: Any,
) : OCondition {

    override fun sql(builder: StringBuilder) {
        builder.append(field).append(" = ?")
    }

    override fun params() = listOf(value)
}

class OContainsCondition(
    val field: String,
    val value: String,
) : OCondition {

    override fun sql(builder: StringBuilder) {
        builder.append(field).append(" containsText ?")
    }

    override fun params() = listOf(value)
}

class OStartsWithCondition(
    val field: String,
    val value: String,
) : OCondition {

    override fun sql(builder: StringBuilder) {
        builder.append(field).append(" like ?")
    }

    override fun params() = listOf("${value}%")
}

// Binary
sealed class OBiCondition(
    val operation: String,
    val left: OCondition,
    val right: OCondition
) : OCondition {

    override fun sql(builder: StringBuilder) {
        builder.append("(")
        left.sql(builder)
        builder.append(" ").append(operation).append(" ")
        right.sql(builder)
        builder.append(")")
    }

    override fun params() = left.params() + right.params()
}

class OAndCondition(left: OCondition, right: OCondition) : OBiCondition("AND", left, right)
class OOrCondition(left: OCondition, right: OCondition) : OBiCondition("OR", left, right)

class OAndNotCondition(
    val left: OCondition,
    val right: OCondition
) : OCondition {

    override fun sql(builder: StringBuilder) {
        builder.append("(")
        left.sql(builder)
        builder.append(" AND NOT (")
        right.sql(builder)
        builder.append("))")
    }

    override fun params() = left.params() + right.params()
}

// Others
class ORangeCondition(
    val field: String,
    val minInclusive: Any,
    val maxInclusive: Any
) : OCondition {

    // https://orientdb.com/docs/3.2.x/sql/SQL-Where.html#between
    override fun sql(builder: StringBuilder) {
        builder.append("(").append(field).append(" between ? and ?)")
    }

    override fun params() = listOf(minInclusive, maxInclusive)
}

class OEdgeExistsCondition(
    val edge: String
) : OCondition {

    override fun sql(builder: StringBuilder) {
        builder.append("outE('").append(edge).append("').size() > 0")
    }
}

class OFieldExistsCondition(
    val field: String
) : OCondition {

    override fun sql(builder: StringBuilder) {
        builder.append("not(").append(field).append(" is null)")
    }
}

class OInstanceOfCondition(
    val className: String,
    val invert: Boolean
) : OCondition {

    override fun sql(builder: StringBuilder) {
        if (invert){
            builder.append("NOT ")
        }
        builder.append("@this INSTANCEOF '$instanceOf'")
    }
}
