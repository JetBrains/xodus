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

sealed interface YTDBOrder : YTDBSql {
    fun merge(newOrder: YTDBOrder): YTDBOrder
    fun reverse(): YTDBOrder
}

data class FieldOrder(val field: String, val ascending: Boolean = true)

class YTDBOrderByFields(
    private val fields: List<FieldOrder>
) : YTDBOrder {

    constructor(field: String, ascending: Boolean = true) : this(
        listOf(
            FieldOrder(
                field,
                ascending
            )
        )
    )

    override fun sql(builder: SqlBuilder) {
        var count = 0

        for ((field, ascending) in fields) {
            if (count++ > 0) {
                builder.append(", ")
            }

            builder.append(field).append(" ").append(if (ascending) "ASC" else "DESC")
        }
    }

    override fun merge(newOrder: YTDBOrder): YTDBOrder {
        return when (newOrder) {
            is YTDBOrderByFields -> {
                val newFields = (newOrder.fields + fields)
                    .distinctBy { it.field } // filter out duplicates in favor of the new order fields
                    .reversed() // reverse to keep the original order of fields
                YTDBOrderByFields(newFields)
            }
            is EmptyOrder -> newOrder
        }
    }

    override fun reverse(): YTDBOrder {
        return YTDBOrderByFields(fields.map { FieldOrder(it.field, !it.ascending) })
    }
}

data object EmptyOrder : YTDBOrder {
    override fun merge(newOrder: YTDBOrder): YTDBOrder {
        return this
    }

    override fun reverse(): YTDBOrder {
        return this
    }

    override fun sql(builder: SqlBuilder) {

    }
}

fun YTDBOrder?.merge(order: YTDBOrder?): YTDBOrder? {
    if (this == null) return order
    if (order == null) return this
    return this.merge(order)
}
