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
import com.orientechnologies.orient.core.metadata.sequence.OSequence
import com.orientechnologies.orient.core.record.OElement
import com.orientechnologies.orient.core.record.ORecord
import com.orientechnologies.orient.core.record.OVertex
import com.orientechnologies.orient.core.sql.executor.OResultSet
import jetbrains.exodus.entitystore.PersistentEntityId
import jetbrains.exodus.entitystore.StoreTransaction

interface OStoreTransaction : StoreTransaction {

    fun getOEntityStore(): OEntityStore

    fun requireActiveTransaction()

    fun requireActiveWritableTransaction()

    fun getTransactionId(): Long

    fun load(id: OEntityId): OVertex?

    fun <T> getRecord(id: ORID): T?
        where T: ORecord

    fun newElement(typeName: String): OElement

    fun newEntityNoSchema(entityType: String, localEntityId: Long): OVertexEntity

    fun delete(id: ORID)

    fun query(sql: String, params: Map<String, Any>): OResultSet

    fun getOEntityId(entityId: PersistentEntityId): OEntityId

    fun getOSequence(sequenceName: String): OSequence

    fun updateOSequence(sequenceName: String, currentValue: Long)

    fun renameOClass(oldName: String, newName: String)
}