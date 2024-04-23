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

sealed interface OLimit : OSql {
    fun min(other: OLimit): OLimit
    fun max(other: OLimit): OLimit
}

class OLimitValue(
    val value: Int
) : OLimit {

    override fun sql() = "$value"

    override fun min(other: OLimit): OLimit {
        return when (other) {
            is OLimitValue -> if (this.value < other.value) this else other
        }
    }

    override fun max(other: OLimit): OLimit {
        return when (other) {
            is OLimitValue -> if (this.value > other.value) this else other
        }
    }
}

fun OLimit?.min(other: OLimit?): OLimit? {
    if (this == null) return other
    if (other == null) return this
    return this.min(other)
}

fun OLimit?.max(other: OLimit?): OLimit? {
    if (this == null) return other
    if (other == null) return this
    return this.max(other)
}