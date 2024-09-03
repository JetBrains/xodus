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
package jetbrains.exodus.entitystore.orientdb

import com.orientechnologies.orient.core.id.OEmptyRecordId
import com.orientechnologies.orient.core.id.ORID
import com.orientechnologies.orient.core.metadata.schema.OClass
import com.orientechnologies.orient.core.record.OVertex
import jetbrains.exodus.entitystore.EntityId

class ORIDEntityId(
    private val classId: Int,
    private val localEntityId: Long,
    private val oId: ORID,
    private val schemaClass: OClass?
) : OEntityId {

    companion object {

        @JvmStatic
        val EMPTY_ID: ORIDEntityId = ORIDEntityId(-1, -1, OEmptyRecordId(), null)

        fun fromVertex(vertex: OVertex): ORIDEntityId {
            val oClass = vertex.requireSchemaClass()
            val classId = oClass.requireClassId()
            val localEntityId = vertex.requireLocalEntityId()
            return ORIDEntityId(classId, localEntityId, vertex.identity, oClass)
        }
    }

    override fun asOId(): ORID {
        return oId
    }

    override fun getTypeId(): Int {
        return classId
    }

    override fun getTypeName(): String {
        return schemaClass?.name ?: "typeNotFound"
    }

    override fun getLocalId(): Long {
        return localEntityId
    }

    override fun compareTo(other: EntityId?): Int {
        if (other !is ORIDEntityId) {
            throw IllegalArgumentException("Cannot compare ORIDEntityId with ${other?.javaClass?.name}")
        }
        return oId.compareTo(other.oId)
    }

    override fun toString(): String {
        return "${classId}-${localEntityId}"
    }

    override fun hashCode(): Int {
        return ((classId shl 20).toLong() xor localEntityId).toInt()
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as ORIDEntityId

        return this.classId == other.classId && this.localEntityId == other.localEntityId
    }
}
