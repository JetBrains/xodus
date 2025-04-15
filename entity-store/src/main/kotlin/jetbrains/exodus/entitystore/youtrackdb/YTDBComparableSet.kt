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
package jetbrains.exodus.entitystore.youtrackdb

import com.jetbrains.youtrack.db.internal.core.db.record.TrackedMultiValue

class YTDBComparableSet<E>(val source: MutableSet<E>) : MutableSet<E> by source, Comparable<MutableSet<E>> {
    val isDirty: Boolean
        get() {
            return if (source is TrackedMultiValue<*, *>) {
                source.isTransactionModified
            } else {
                true
            }
        }

    override fun compareTo(other: MutableSet<E>): Int {
        return 0
    }
}
