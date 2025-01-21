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
package jetbrains.exodus.entitystore.youtrackdb.query


// Where
sealed interface YTDBCondition : YTDBQuery

// Property
class YTDBEqualCondition(
    val field: String,
    val value: Any,
) : YTDBCondition {

    override fun sql(builder: SqlBuilder) {
        val param = builder.addParam(field, value)
        builder.append(field).append(" = :$param")
    }
}

class YTDBContainsCondition(
    val field: String,
    val value: String,
    val ignoreCase: Boolean,
) : YTDBCondition {

    override fun sql(builder: SqlBuilder) {
        val param = if (ignoreCase){
            builder.addParam(field, value.lowercase())
        } else{
            builder.addParam(field, value)
        }
        builder.append(field).let { builder -> if (ignoreCase) builder.append(".toLowerCase()") else builder }.append(" containsText :$param")
    }
}

class YTDBStartsWithCondition(
    val field: String,
    val value: String,
) : YTDBCondition {

    override fun sql(builder: SqlBuilder) {
        val param = builder.addParam(field, "${value.lowercase()}%")
        builder.append(field).append(" like :$param")
    }
}

class YTDBFieldExistsCondition(
    val field: String
) : YTDBCondition {

    override fun sql(builder: SqlBuilder) {
        builder.append("not(").append(field).append(" is null)")
    }
}

class YTDBFieldIsNullCondition(
    val field: String
) : YTDBCondition {

    override fun sql(builder: SqlBuilder) {
        builder.append(field).append(" is null")
    }
}

// Edge
class YTDBEdgeExistsCondition(
    val edge: String
) : YTDBCondition {

    override fun sql(builder: SqlBuilder) {
        builder.append("outE('").append(edge).append("').size() > 0")
    }
}

class YTDBEdgeIsNullCondition(
    val edge: String
) : YTDBCondition {

    override fun sql(builder: SqlBuilder) {
        builder.append("outE('").append(edge).append("').size() == 0")
    }
}

// Binary
sealed class YTDBBiCondition(
    val operation: String,
    val left: YTDBCondition,
    val right: YTDBCondition
) : YTDBCondition {

    override fun sql(builder: SqlBuilder) {
        builder.append("(")
        left.sql(builder)
        builder.append(" ").append(operation).append(" ")
        right.sql(builder)
        builder.append(")")
    }
}

class YTDBAndCondition(left: YTDBCondition, right: YTDBCondition) : YTDBBiCondition("AND", left, right)
class YTDBOrCondition(left: YTDBCondition, right: YTDBCondition) : YTDBBiCondition("OR", left, right)

// Negation
class NotCondition(
    val condition: YTDBCondition
) : YTDBCondition {

    override fun sql(builder: SqlBuilder) {
        builder.append("NOT (")
        condition.sql(builder)
        builder.append(")")
    }
}

class YTDBAndNotCondition(
    val left: YTDBCondition,
    val right: YTDBCondition
) : YTDBCondition {

    override fun sql(builder: SqlBuilder) {
        builder.append("(")
        left.sql(builder)
        builder.append(" AND NOT (")
        right.sql(builder)
        builder.append("))")
    }
}

// Others
class YTDBRangeCondition(
    val field: String,
    val minInclusive: Any,
    val maxInclusive: Any
) : YTDBCondition {

    // https://orientdb.com/docs/3.2.x/sql/SQL-Where.html#between
    override fun sql(builder: SqlBuilder) {
        val left = builder.addParam("min", minInclusive)
        val right = builder.addParam("max", maxInclusive)
        builder.append("(").append(field).append(" between :$left and :$right)")
    }
}

class YTDBInstanceOfCondition(
    val className: String,
    val invert: Boolean
) : YTDBCondition {

    override fun sql(builder: SqlBuilder) {
        if (invert) {
            builder.append("NOT ")
        }
        builder.append("@this INSTANCEOF '$className'")
    }
}
