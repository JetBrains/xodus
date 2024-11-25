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

    override fun sql(builder: SqlBuilder) {
        val param = builder.addParam(field, value)
        builder.append(field).append(" = :$param")
    }
}

class OContainsCondition(
    val field: String,
    val value: String,
    val ignoreCase: Boolean,
) : OCondition {

    override fun sql(builder: SqlBuilder) {
        val param = if (ignoreCase){
            builder.addParam(field, value.lowercase())
        } else{
            builder.addParam(field, value)
        }
        builder.append(field).let { builder -> if (ignoreCase) builder.append(".toLowerCase()") else builder }.append(" containsText :$param")
    }
}

class OStartsWithCondition(
    val field: String,
    val value: String,
) : OCondition {

    override fun sql(builder: SqlBuilder) {
        val param = builder.addParam(field, "${value.lowercase()}%")
        builder.append(field).append(" like :$param")
    }
}

class OFieldExistsCondition(
    val field: String
) : OCondition {

    override fun sql(builder: SqlBuilder) {
        builder.append("not(").append(field).append(" is null)")
    }
}

class OFieldIsNullCondition(
    val field: String
) : OCondition {

    override fun sql(builder: SqlBuilder) {
        builder.append(field).append(" is null")
    }
}

// Edge
class OEdgeExistsCondition(
    val edge: String
) : OCondition {

    override fun sql(builder: SqlBuilder) {
        builder.append("outE('").append(edge).append("').size() > 0")
    }
}

class OEdgeIsNullCondition(
    val edge: String
) : OCondition {

    override fun sql(builder: SqlBuilder) {
        builder.append("outE('").append(edge).append("').size() == 0")
    }
}

// Binary
sealed class OBiCondition(
    val operation: String,
    val left: OCondition,
    val right: OCondition
) : OCondition {

    override fun sql(builder: SqlBuilder) {
        builder.append("(")
        left.sql(builder)
        builder.append(" ").append(operation).append(" ")
        right.sql(builder)
        builder.append(")")
    }
}

class OAndCondition(left: OCondition, right: OCondition) : OBiCondition("AND", left, right)
class OOrCondition(left: OCondition, right: OCondition) : OBiCondition("OR", left, right)

// Negation
class NotCondition(
    val condition: OCondition
) : OCondition {

    override fun sql(builder: SqlBuilder) {
        builder.append("NOT (")
        condition.sql(builder)
        builder.append(")")
    }
}

class OAndNotCondition(
    val left: OCondition,
    val right: OCondition
) : OCondition {

    override fun sql(builder: SqlBuilder) {
        builder.append("(")
        left.sql(builder)
        builder.append(" AND NOT (")
        right.sql(builder)
        builder.append("))")
    }
}

// Others
class ORangeCondition(
    val field: String,
    val minInclusive: Any,
    val maxInclusive: Any
) : OCondition {

    // https://orientdb.com/docs/3.2.x/sql/SQL-Where.html#between
    override fun sql(builder: SqlBuilder) {
        val left = builder.addParam("min", minInclusive)
        val right = builder.addParam("max", maxInclusive)
        builder.append("(").append(field).append(" between :$left and :$right)")
    }
}

class OInstanceOfCondition(
    val className: String,
    val invert: Boolean
) : OCondition {

    override fun sql(builder: SqlBuilder) {
        if (invert) {
            builder.append("NOT ")
        }
        builder.append("@this INSTANCEOF '$className'")
    }
}
