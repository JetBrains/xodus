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

import com.orientechnologies.orient.core.id.ORID
import com.orientechnologies.orient.core.metadata.schema.OClass
import com.orientechnologies.orient.core.record.OVertex
import jetbrains.exodus.entitystore.EntityId

class ORIDEntityId(private val id: ORID, private val schemaClass: OClass) : OEntityId {

    companion object {

        fun fromVertex(vertex: OVertex): ORIDEntityId {
            val schema = vertex.schemaClass
            require(schema != null) { "Vertex $vertex must have a schema class" }
            return ORIDEntityId(vertex.identity, schema)
        }
    }

    @Volatile
    private var cachedLocalId: Long = -1

    override fun asOId(): ORID {
        return id
    }

    override fun getTypeId(): Int {
        // Default cluster id is always the same for the same class
        // Can not use class.name as it might be changed from the outside
        return schemaClass.defaultClusterId
    }

    fun getTypeName(): String {
        return schemaClass.name
    }

    override fun getLocalId(): Long {
        if (cachedLocalId != -1L){
            return cachedLocalId
        }
        /*
        The idea is that (localId % clusterSize) is equal for all records from the same cluster.
        For records from different clusters it's not equal. Cluster ids assumed to be increasing with delta = 1.
         */
        val clustersCount = schemaClass.clusterIds.size
        val div = id.clusterId - schemaClass.defaultClusterId
        cachedLocalId = div + clustersCount * id.clusterPosition
        return cachedLocalId
    }

    override fun compareTo(other: EntityId?): Int {
        if (other !is ORIDEntityId) {
            throw IllegalArgumentException("Cannot compare ORIDEntityId with ${other?.javaClass?.name}")
        }
        return id.compareTo(other.id)
    }

    override fun toString(): String {
        return "${schemaClass}:${id}"
    }

    override fun hashCode(): Int {
        return id.hashCode()
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as ORIDEntityId

        return id == other.id
    }


}
