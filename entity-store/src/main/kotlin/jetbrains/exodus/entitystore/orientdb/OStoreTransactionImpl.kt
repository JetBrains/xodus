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

import com.orientechnologies.orient.core.db.ODatabaseSession
import com.orientechnologies.orient.core.db.document.ODatabaseDocument
import com.orientechnologies.orient.core.record.OVertex
import com.orientechnologies.orient.core.tx.OTransaction
import jetbrains.exodus.entitystore.*
import jetbrains.exodus.entitystore.iterate.property.*
import jetbrains.exodus.entitystore.orientdb.iterate.OEntityOfTypeIterable
import jetbrains.exodus.entitystore.orientdb.iterate.link.*
import jetbrains.exodus.entitystore.orientdb.iterate.property.OPropertyEqualIterable
import jetbrains.exodus.entitystore.orientdb.iterate.property.getOrCreateSequence
import jetbrains.exodus.entitystore.orientdb.query.OQueryCancellingPolicy
import jetbrains.exodus.env.Transaction

class OStoreTransactionImpl(
    override val activeSession: ODatabaseSession,
    private val store: PersistentEntityStore,
    private val schemaBuddy: OSchemaBuddy,
    private val sessionCreator: () -> ODatabaseDocument
) : OStoreTransaction {

    private var queryCancellingPolicy: OQueryCancellingPolicy? = null

    private var hasWriteOperations = false

    private fun getCurrentTx(): OTransaction? = activeSession.transaction

    override fun getStore(): EntityStore {
        return store
    }

    override fun isIdempotent(): Boolean {
        val operations = getCurrentTx()?.recordOperations ?: listOf()
        return operations.firstOrNull() == null
    }

    override fun isReadonly(): Boolean {
        if (!hasWriteOperations) {
            hasWriteOperations = !isIdempotent()
        }
        return !hasWriteOperations
    }

    override fun isFinished(): Boolean {
        return !activeSession.hasActiveTransaction()
    }

    override fun commit(): Boolean {
        try {
            activeSession.commit()
        } finally {
            cleanUpTxIfNeeded()
        }

        return true
    }

    override fun isCurrent(): Boolean {
        return true
    }

    override fun abort() {
        try {
            activeSession.rollback()
        } finally {
            cleanUpTxIfNeeded()
        }
    }

    override fun flush(): Boolean {
        try {
            activeSession.commit()
            activeSession.begin()
        } finally {
            cleanUpTxIfNeeded()
        }

        return true
    }

    private fun cleanUpTxIfNeeded() {
        if (!activeSession.hasActiveTransaction()) {
            (store as OPersistentEntityStore).completeTx()
            activeSession.close()
        }
    }

    override fun revert() {
        try {
            activeSession.rollback()
            activeSession.begin()
        } finally {
            cleanUpTxIfNeeded()
        }
    }

    override fun getSnapshot(): StoreTransaction {
        return this
    }

    override fun newEntity(entityType: String): Entity {
        schemaBuddy.makeSureTypeExists(activeSession, entityType)
        val vertex = activeSession.newVertex(entityType)
        activeSession.setLocalEntityId(entityType, vertex)
        vertex.save<OVertex>()
        return OVertexEntity(vertex, store)
    }

    override fun saveEntity(entity: Entity) {
        require(entity is OVertexEntity) { "Only OVertexEntity is supported, but was ${entity.javaClass.simpleName}" }
        entity.save()
    }

    override fun getEntity(id: EntityId): Entity {
        val oId = store.requireOEntityId(id)
        if (oId == ORIDEntityId.EMPTY_ID) {
            throw EntityRemovedInDatabaseException(oId.getTypeName(), id)
        }
        val vertex: OVertex = activeSession.load(oId.asOId()) ?: throw EntityRemovedInDatabaseException(oId.getTypeName(), id)
        return OVertexEntity(vertex, store)
    }

    override fun getEntityTypes(): MutableList<String> {
        return activeSession.metadata.schema.classes.map { it.name }.toMutableList()
    }

    override fun getAll(entityType: String): EntityIterable {
        return OEntityOfTypeIterable(this, entityType)
    }

    override fun getSingletonIterable(entity: Entity): EntityIterable {
        return OSingleEntityIterable(this, entity)
    }

    override fun find(entityType: String, propertyName: String, value: Comparable<Nothing>): EntityIterable {
        return OPropertyEqualIterable(this, entityType, propertyName, value)
    }

    override fun find(
        entityType: String,
        propertyName: String,
        minValue: Comparable<Nothing>,
        maxValue: Comparable<Nothing>
    ): EntityIterable {
        return OPropertyRangeIterable(this, entityType, propertyName, minValue, maxValue)
    }

    override fun findContaining(
        entityType: String,
        propertyName: String,
        value: String,
        ignoreCase: Boolean
    ): EntityIterable {
        return OPropertyContainsIterable(this, entityType, propertyName, value)
    }

    override fun findStartingWith(entityType: String, propertyName: String, value: String): EntityIterable {
        return OPropertyStartsWithIterable(this, entityType, propertyName, value)
    }

    override fun findIds(entityType: String, minValue: Long, maxValue: Long): EntityIterable {
        return OPropertyRangeIterable(
            this,
            entityType,
            OVertexEntity.LOCAL_ENTITY_ID_PROPERTY_NAME,
            minValue,
            maxValue
        )
    }

    override fun findWithProp(entityType: String, propertyName: String): EntityIterable {
        return OPropertyExistsIterable(this, entityType, propertyName)
    }

    override fun findWithPropSortedByValue(entityType: String, propertyName: String): EntityIterable {
        return OPropertyExistsSortedIterable(this, entityType, propertyName)
    }

    override fun findWithBlob(entityType: String, blobName: String): EntityIterable {
        return OPropertyBlobExistsEntityIterable(this, entityType, blobName)
    }

    override fun findLinks(entityType: String, entity: Entity, linkName: String): EntityIterable {
        return OLinkOfTypeToEntityIterable(this, linkName, entity.id as OEntityId, entityType)
    }

    override fun findLinks(entityType: String, entities: EntityIterable, linkName: String): EntityIterable {
        return OLinkIterableToEntityIterable(this, entities.asOQueryIterable(), linkName)
    }

    override fun findWithLinks(entityType: String, linkName: String): EntityIterable {
        return OLinkExistsEntityIterable(this, entityType, linkName)
    }

    override fun findWithLinks(
        entityType: String,
        linkName: String,
        oppositeEntityType: String,
        oppositeLinkName: String
    ): EntityIterable {
        return OLinkExistsEntityIterable(this, entityType, linkName)
    }

    override fun sort(entityType: String, propertyName: String, ascending: Boolean): EntityIterable {
        return OPropertySortedIterable(this, entityType, propertyName, null, ascending)
    }

    override fun sort(
        entityType: String,
        propertyName: String,
        rightOrder: EntityIterable,
        ascending: Boolean
    ): EntityIterable {
        return OPropertySortedIterable(this, entityType, propertyName, rightOrder.asOQueryIterable(), ascending)
    }

    override fun sortLinks(
        entityType: String,
        sortedLinks: EntityIterable,
        isMultiple: Boolean,
        linkName: String,
        rightOrder: EntityIterable
    ): EntityIterable {
        return OLinkSortEntityIterable(this, sortedLinks.asOQueryIterable(), linkName, rightOrder.asOQueryIterable())
    }

    override fun sortLinks(
        entityType: String,
        sortedLinks: EntityIterable,
        isMultiple: Boolean,
        linkName: String,
        rightOrder: EntityIterable,
        oppositeEntityType: String,
        oppositeLinkName: String
    ): EntityIterable {
        // Not sure about skipping oppositeEntityType and oppositeLinkName values
        return OLinkSortEntityIterable(this, sortedLinks.asOQueryIterable(), linkName, rightOrder.asOQueryIterable())
    }

    @Deprecated("Deprecated in Java")
    override fun mergeSorted(sorted: MutableList<EntityIterable>, comparator: Comparator<Entity>): EntityIterable {
        throw UnsupportedOperationException("Not implemented")
    }

    override fun mergeSorted(
        sorted: List<EntityIterable>,
        valueGetter: ComparableGetter,
        comparator: java.util.Comparator<Comparable<Any>?>
    ): EntityIterable {
        throw UnsupportedOperationException("Not implemented")
    }

    override fun toEntityId(representation: String): EntityId {
        val legacyId = PersistentEntityId.toEntityId(representation)
        return store.requireOEntityId(legacyId)
    }

    override fun getSequence(sequenceName: String): Sequence {
        return activeSession.getOrCreateSequence(sequenceName, sessionCreator, -1)
    }

    override fun getSequence(sequenceName: String, initialValue: Long): Sequence {
        return activeSession.getOrCreateSequence(sequenceName, sessionCreator, initialValue)
    }

    override fun getEnvironmentTransaction(): Transaction {
        return OEnvironmentTransaction(store.environment, this)
    }

    override fun setQueryCancellingPolicy(policy: QueryCancellingPolicy?) {
        require(policy is OQueryCancellingPolicy) { "Only OQueryCancellingPolicy is supported, but was ${policy?.javaClass?.simpleName}" }
        this.queryCancellingPolicy = policy
    }

    override fun getQueryCancellingPolicy() = this.queryCancellingPolicy
}
