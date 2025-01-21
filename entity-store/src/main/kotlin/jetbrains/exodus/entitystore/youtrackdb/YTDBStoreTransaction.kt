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

import com.jetbrains.youtrack.db.api.DatabaseSession
import com.jetbrains.youtrack.db.api.query.ResultSet
import com.jetbrains.youtrack.db.api.record.DBRecord
import com.jetbrains.youtrack.db.api.record.Vertex
import com.jetbrains.youtrack.db.api.schema.SchemaClass
import com.jetbrains.youtrack.db.internal.core.metadata.sequence.DBSequence
import jetbrains.exodus.entitystore.PersistentEntityId
import jetbrains.exodus.entitystore.StoreTransaction

interface YTDBStoreTransaction : StoreTransaction {
    val databaseSession: DatabaseSession

    fun getOEntityStore(): YTDBEntityStore

    fun getTransactionId(): Long

    fun requireActiveTransaction()

    fun requireActiveWritableTransaction()

    fun deactivateOnCurrentThread()

    fun activateOnCurrentThread()


    fun <T> getRecord(id: YTDBEntityId): T
            where T : DBRecord

    fun newEntity(entityType: String, localEntityId: Long): YTDBVertexEntity

    fun generateEntityId(entityType: String, vertex: Vertex)

    fun bindToSession(vertex: Vertex): Vertex

    fun bindToSession(entity: YTDBVertexEntity): YTDBVertexEntity

    fun query(sql: String, params: Map<String, Any>): ResultSet

    fun getOEntityId(entityId: PersistentEntityId): YTDBEntityId

    /**
     * If the class has not been found, returns -1. It is how it was in the Classic Xodus.
     */
    fun getTypeId(entityType: String): Int

    /**
    If the class has not been found, will throw EntityRemovedInDatabaseException with invalid type id
     */
    fun getType(entityTypeId: Int): String

    fun getOSequence(sequenceName: String): DBSequence

    fun updateOSequence(sequenceName: String, currentValue: Long)

    fun renameOClass(oldName: String, newName: String)

    fun getOrCreateEdgeClass(
        linkName: String,
        outClassName: String,
        inClassName: String
    ): SchemaClass

    fun bindResultSet(resultSet: ResultSet)
}
