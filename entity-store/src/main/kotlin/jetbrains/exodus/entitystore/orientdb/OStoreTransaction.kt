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

import com.orientechnologies.orient.core.metadata.schema.OClass
import com.orientechnologies.orient.core.metadata.sequence.OSequence
import com.orientechnologies.orient.core.record.ORecord
import com.orientechnologies.orient.core.record.OVertex
import com.orientechnologies.orient.core.sql.executor.OResultSet
import jetbrains.exodus.entitystore.PersistentEntityId
import jetbrains.exodus.entitystore.StoreTransaction

interface OStoreTransaction : StoreTransaction {

    fun getOEntityStore(): OEntityStore

    fun getTransactionId(): Long


    fun requireActiveTransaction()

    fun requireActiveWritableTransaction()

    fun deactivateOnCurrentThread()

    fun activateOnCurrentThread()


    fun <T> getRecord(id: OEntityId): T?
        where T: ORecord

    fun newEntity(entityType: String, localEntityId: Long): OVertexEntity

    fun generateEntityId(entityType: String, vertex: OVertex)

    fun bindToSession(vertex: OVertex):OVertex

    fun query(sql: String, params: Map<String, Any>): OResultSet

    fun getOEntityId(entityId: PersistentEntityId): OEntityId

    /**
     * If the class has not been found, returns -1. It is how it was in the Classic Xodus.
     */
    fun getTypeId(entityType: String): Int

    fun getOSequence(sequenceName: String): OSequence

    fun updateOSequence(sequenceName: String, currentValue: Long)

    fun renameOClass(oldName: String, newName: String)

    fun getOrCreateEdgeClass(linkName: String, outClassName: String, inClassName: String): OClass
}