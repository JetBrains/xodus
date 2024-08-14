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

import com.orientechnologies.orient.core.db.ODatabase
import com.orientechnologies.orient.core.db.ODatabaseSession
import com.orientechnologies.orient.core.record.OVertex
import com.orientechnologies.orient.core.sql.executor.OResultSet
import com.orientechnologies.orient.core.tx.OTransaction.TXSTATUS
import com.orientechnologies.orient.core.tx.OTransactionNoTx
import jetbrains.exodus.entitystore.*
import jetbrains.exodus.entitystore.iterate.property.*
import jetbrains.exodus.entitystore.orientdb.iterate.OEntityOfTypeIterable
import jetbrains.exodus.entitystore.orientdb.iterate.link.*
import jetbrains.exodus.entitystore.orientdb.iterate.property.OPropertyEqualIterable
import jetbrains.exodus.entitystore.orientdb.iterate.property.getOrCreateSequence
import jetbrains.exodus.entitystore.orientdb.query.OQueryCancellingPolicy
import jetbrains.exodus.env.Transaction

typealias InSessionExecutor = (ODatabaseSession.() -> Unit) -> Unit
typealias OnTransactionFinishedHandler = (ODatabaseSession, OStoreTransaction) -> Unit

class OStoreTransactionImpl(
    override val activeSession: ODatabaseSession,
    private val store: OPersistentEntityStore,
    private val schemaBuddy: OSchemaBuddy,
    private val executeInASeparateSession: InSessionExecutor,
    private val onFinished: OnTransactionFinishedHandler,
    private val readOnly: Boolean = false
) : OStoreTransaction {
    private var queryCancellingPolicy: OQueryCancellingPolicy? = null

    // todo test
    override fun getTransactionId(): Long {
        return activeSession.transaction.id.toLong()
    }

    // todo test
    override fun load(id: OEntityId): OVertex? {
        requireActiveTx()
        return activeSession.load(id.asOId())
    }

    // todo test
    override fun query(sql: String, params: List<Any>): OResultSet {
        requireActiveTx()
        return activeSession.query(sql, *params.toTypedArray())
    }

    override fun getStore(): EntityStore {
        return store
    }

    override fun isIdempotent(): Boolean {
        return readOnly || activeSession.transaction.recordOperations.none()
    }

    override fun isReadonly(): Boolean {
        return readOnly
    }

    override fun isFinished(): Boolean {
        return activeSession.status == ODatabase.STATUS.CLOSED
    }

    override fun isCurrent(): Boolean {
        return !isFinished && activeSession.isActiveOnCurrentThread
    }

    private fun requireActiveTx() {
        check(!isFinished) { "The transaction is finished, the internal session state: ${activeSession.status}" }
        check(activeSession.isActiveOnCurrentThread) { "The active session is no the session the transaction was started in" }
        check(activeSession.transaction.status == TXSTATUS.BEGUN) { "The current OTransaction status is ${activeSession.transaction.status}, but the status ${TXSTATUS.BEGUN} was expected." }
    }

    private fun requireWritableAndActiveTx() {
        check(!readOnly) { "Can't modify temporary empty store" }
        requireActiveTx()
    }

    fun begin() {
        check(activeSession.status == ODatabase.STATUS.OPEN) { "The session status is ${activeSession.status}. But ${ODatabase.STATUS.OPEN} is required." }
        check(activeSession.isActiveOnCurrentThread) { "The session is not active on the current thread" }
        check(activeSession.transaction is OTransactionNoTx) { "The session must not have a transaction" }
        try {
            activeSession.begin()
        } finally {
            cleanUpTxIfNeeded()
        }
    }

    override fun commit(): Boolean {
        requireActiveTx()
        try {
            activeSession.commit()
        } finally {
            cleanUpTxIfNeeded()
        }

        return true
    }

    override fun flush(): Boolean {
        requireActiveTx()
        try {
            activeSession.commit()
            activeSession.begin()
        } finally {
            cleanUpTxIfNeeded()
        }

        return true
    }

    override fun abort() {
        requireActiveTx()
        try {
            activeSession.rollback()
        } finally {
            cleanUpTxIfNeeded()
        }
    }

    override fun revert() {
        requireActiveTx()
        try {
            activeSession.rollback()
            activeSession.begin()
        } finally {
            cleanUpTxIfNeeded()
        }
    }

    private fun cleanUpTxIfNeeded() {
        if (activeSession.status == ODatabase.STATUS.OPEN && activeSession.transaction.status == TXSTATUS.INVALID) {
            onFinished(activeSession, this)
        }
    }

    override fun getSnapshot(): StoreTransaction {
        return this
    }

    override fun newEntity(entityType: String): Entity {
        requireWritableAndActiveTx()
        schemaBuddy.makeSureTypeExists(activeSession, entityType)
        val vertex = activeSession.newVertex(entityType)
        activeSession.setLocalEntityId(entityType, vertex)
        vertex.save<OVertex>()
        return OVertexEntity(vertex, store)
    }

    override fun saveEntity(entity: Entity) {
        require(entity is OVertexEntity) { "Only OVertexEntity is supported, but was ${entity.javaClass.simpleName}" }
        requireWritableAndActiveTx()
        entity.save()
    }

    override fun getEntity(id: EntityId): Entity {
        requireActiveTx()
        val oId = store.requireOEntityId(id)
        if (oId == ORIDEntityId.EMPTY_ID) {
            throw EntityRemovedInDatabaseException(oId.getTypeName(), id)
        }
        val vertex: OVertex = activeSession.load(oId.asOId()) ?: throw EntityRemovedInDatabaseException(oId.getTypeName(), id)
        return OVertexEntity(vertex, store)
    }

    override fun getEntityTypes(): MutableList<String> {
        requireActiveTx()
        return activeSession.metadata.schema.classes.map { it.name }.toMutableList()
    }

    override fun getAll(entityType: String): EntityIterable {
        requireActiveTx()
        return OEntityOfTypeIterable(this, entityType)
    }

    override fun getSingletonIterable(entity: Entity): EntityIterable {
        requireActiveTx()
        return OSingleEntityIterable(this, entity)
    }

    override fun find(entityType: String, propertyName: String, value: Comparable<Nothing>): EntityIterable {
        requireActiveTx()
        return OPropertyEqualIterable(this, entityType, propertyName, value)
    }

    override fun find(
        entityType: String,
        propertyName: String,
        minValue: Comparable<Nothing>,
        maxValue: Comparable<Nothing>
    ): EntityIterable {
        requireActiveTx()
        return OPropertyRangeIterable(this, entityType, propertyName, minValue, maxValue)
    }

    override fun findContaining(
        entityType: String,
        propertyName: String,
        value: String,
        ignoreCase: Boolean
    ): EntityIterable {
        requireActiveTx()
        return OPropertyContainsIterable(this, entityType, propertyName, value)
    }

    override fun findStartingWith(entityType: String, propertyName: String, value: String): EntityIterable {
        requireActiveTx()
        return OPropertyStartsWithIterable(this, entityType, propertyName, value)
    }

    override fun findIds(entityType: String, minValue: Long, maxValue: Long): EntityIterable {
        requireActiveTx()
        return OPropertyRangeIterable(
            this,
            entityType,
            OVertexEntity.LOCAL_ENTITY_ID_PROPERTY_NAME,
            minValue,
            maxValue
        )
    }

    override fun findWithProp(entityType: String, propertyName: String): EntityIterable {
        requireActiveTx()
        return OPropertyExistsIterable(this, entityType, propertyName)
    }

    override fun findWithPropSortedByValue(entityType: String, propertyName: String): EntityIterable {
        requireActiveTx()
        return OPropertyExistsSortedIterable(this, entityType, propertyName)
    }

    override fun findWithBlob(entityType: String, blobName: String): EntityIterable {
        requireActiveTx()
        return OPropertyBlobExistsEntityIterable(this, entityType, blobName)
    }

    override fun findLinks(entityType: String, entity: Entity, linkName: String): EntityIterable {
        requireActiveTx()
        return OLinkOfTypeToEntityIterable(this, linkName, entity.id as OEntityId, entityType)
    }

    override fun findLinks(entityType: String, entities: EntityIterable, linkName: String): EntityIterable {
        requireActiveTx()
        return OLinkIterableToEntityIterable(this, entities.asOQueryIterable(), linkName)
    }

    override fun findWithLinks(entityType: String, linkName: String): EntityIterable {
        requireActiveTx()
        return OLinkExistsEntityIterable(this, entityType, linkName)
    }

    override fun findWithLinks(
        entityType: String,
        linkName: String,
        oppositeEntityType: String,
        oppositeLinkName: String
    ): EntityIterable {
        requireActiveTx()
        return OLinkExistsEntityIterable(this, entityType, linkName)
    }

    override fun sort(entityType: String, propertyName: String, ascending: Boolean): EntityIterable {
        requireActiveTx()
        return OPropertySortedIterable(this, entityType, propertyName, null, ascending)
    }

    override fun sort(
        entityType: String,
        propertyName: String,
        rightOrder: EntityIterable,
        ascending: Boolean
    ): EntityIterable {
        requireActiveTx()
        return OPropertySortedIterable(this, entityType, propertyName, rightOrder.asOQueryIterable(), ascending)
    }

    override fun sortLinks(
        entityType: String,
        sortedLinks: EntityIterable,
        isMultiple: Boolean,
        linkName: String,
        rightOrder: EntityIterable
    ): EntityIterable {
        requireActiveTx()
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
        requireActiveTx()
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
        requireActiveTx()
        val legacyId = PersistentEntityId.toEntityId(representation)
        return store.requireOEntityId(legacyId)
    }

    override fun getSequence(sequenceName: String): Sequence {
        requireActiveTx()
        return activeSession.getOrCreateSequence(sequenceName, executeInASeparateSession, -1)
    }

    override fun getSequence(sequenceName: String, initialValue: Long): Sequence {
        requireActiveTx()
        return activeSession.getOrCreateSequence(sequenceName, executeInASeparateSession, initialValue)
    }

    override fun getEnvironmentTransaction(): Transaction {
        requireActiveTx()
        return OEnvironmentTransaction(store.environment, this)
    }

    override fun setQueryCancellingPolicy(policy: QueryCancellingPolicy?) {
        require(policy is OQueryCancellingPolicy) { "Only OQueryCancellingPolicy is supported, but was ${policy?.javaClass?.simpleName}" }
        this.queryCancellingPolicy = policy
    }

    override fun getQueryCancellingPolicy() = this.queryCancellingPolicy
}
