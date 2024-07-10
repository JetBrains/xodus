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

sealed interface OOrder : OSql {
    fun merge(newOrder: OOrder): OOrder
    fun reverse(): OOrder
}

data class FieldOrder(val field: String, val ascending: Boolean = true)

class OOrderByFields(
    private val fields: List<FieldOrder>
) : OOrder {

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

    override fun merge(newOrder: OOrder): OOrder {
        return when (newOrder) {
            is OOrderByFields -> {
                val newFields = (newOrder.fields + fields)
                    .distinctBy { it.field } // filter out duplicates in favor of the new order
                OOrderByFields(newFields)
            }
        }
    }

    override fun reverse(): OOrder {
        return OOrderByFields(fields.map { FieldOrder(it.field, !it.ascending) })
    }
}

fun OOrder?.merge(order: OOrder?): OOrder? {
    if (this == null) return order
    if (order == null) return this
    return this.merge(order)
}