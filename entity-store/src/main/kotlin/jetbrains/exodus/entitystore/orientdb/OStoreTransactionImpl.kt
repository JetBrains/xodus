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
import com.orientechnologies.orient.core.id.ORID
import com.orientechnologies.orient.core.metadata.sequence.OSequence
import com.orientechnologies.orient.core.record.OElement
import com.orientechnologies.orient.core.record.ORecord
import com.orientechnologies.orient.core.record.OVertex
import com.orientechnologies.orient.core.sql.executor.OResultSet
import com.orientechnologies.orient.core.tx.OTransaction.TXSTATUS
import com.orientechnologies.orient.core.tx.OTransactionNoTx
import jetbrains.exodus.entitystore.*
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.LOCAL_ENTITY_ID_PROPERTY_NAME
import jetbrains.exodus.entitystore.orientdb.iterate.OEntityOfTypeIterable
import jetbrains.exodus.entitystore.orientdb.iterate.link.*
import jetbrains.exodus.entitystore.orientdb.iterate.property.*
import jetbrains.exodus.entitystore.orientdb.query.OQueryCancellingPolicy
import jetbrains.exodus.env.Transaction

internal typealias TransactionEventHandler = (ODatabaseSession, OStoreTransaction) -> Unit

class OStoreTransactionImpl(
    private val session: ODatabaseSession,
    private val store: OPersistentEntityStore,
    private val schemaBuddy: OSchemaBuddy,
    private val onFinished: TransactionEventHandler,
    private val onDeactivated: TransactionEventHandler,
    private val onActivated: TransactionEventHandler,
    private val readOnly: Boolean = false
) : OStoreTransaction {
    private var queryCancellingPolicy: OQueryCancellingPolicy? = null

    /**
     * The Orient transaction gets changed on flush(), so its id gets changed too.
     * It would be strange if id of OStoreTransaction gets changed during its lifetime,
     * so it was decided to remember the first Orient transaction id and use it as OStoreTransaction id.
     *
     * If you think that it should be implemented differently, come and let's discuss.
     */
    private val transactionIdImpl by lazy {
        requireActiveTransaction()
        session.transaction.id.toLong()
    }

    override fun getTransactionId(): Long {
        return transactionIdImpl
    }

    override fun <T> getRecord(id: OEntityId): T?
    where T: ORecord {
        requireActiveTransaction()
        return session.getRecord(id.asOId())
    }

    override fun newElement(typeName: String): OElement {
        requireActiveWritableTransaction()
        schemaBuddy.requireTypeExists(session, typeName)
        return session.newElement(typeName)
    }

    override fun delete(id: ORID) {
        requireActiveWritableTransaction()
        session.delete(id)
    }

    // todo test
    override fun query(sql: String, params: Map<String, Any>): OResultSet {
        requireActiveTransaction()
        return session.query(sql, params)
    }

    override fun getStore(): EntityStore {
        return store
    }

    override fun isIdempotent(): Boolean {
        return readOnly || session.transaction.recordOperations.none()
    }

    override fun isReadonly(): Boolean {
        return readOnly
    }

    override fun isFinished(): Boolean {
        return session.status == ODatabase.STATUS.CLOSED
    }

    override fun isCurrent(): Boolean {
        return !isFinished && session.isActiveOnCurrentThread
    }

    override fun getOEntityStore(): OEntityStore {
        return store
    }

    override fun requireActiveTransaction() {
        check(session.status == ODatabase.STATUS.OPEN) { "The transaction is finished, the internal session state: ${session.status}" }
        check(session.isActiveOnCurrentThread) { "The active session is no the session the transaction was started in" }
        check(session.transaction.status == TXSTATUS.BEGUN) { "The current OTransaction status is ${session.transaction.status}, but the status ${TXSTATUS.BEGUN} was expected." }
    }

    override fun requireActiveWritableTransaction() {
        check(!readOnly) { "Cannot modify read-only transaction" }
        requireActiveTransaction()
    }

    override fun deactivateOnCurrentThread() {
        requireActiveTransaction()
        onDeactivated(session, this)
    }

    override fun activateOnCurrentThread() {
        check(session.status == ODatabase.STATUS.OPEN) { "The transaction is finished, the internal session state: ${session.status}" }
        check(!session.isActiveOnCurrentThread) { "The transaction is already active on the current thread" }
        onActivated(session, this)
    }

    fun begin() {
        check(session.status == ODatabase.STATUS.OPEN) { "The session status is ${session.status}. But ${ODatabase.STATUS.OPEN} is required." }
        check(session.isActiveOnCurrentThread) { "The session is not active on the current thread" }
        check(session.transaction is OTransactionNoTx) { "The session must not have a transaction" }
        try {
            session.begin()
            // initialize transaction id
            transactionIdImpl
        } finally {
            cleanUpTxIfNeeded()
        }
    }

    override fun commit(): Boolean {
        requireActiveTransaction()
        try {
            session.commit()
        } finally {
            cleanUpTxIfNeeded()
        }

        return true
    }

    override fun flush(): Boolean {
        requireActiveTransaction()
        try {
            session.commit()
            session.begin()
        } finally {
            cleanUpTxIfNeeded()
        }

        return true
    }

    override fun abort() {
        requireActiveTransaction()
        try {
            session.rollback()
        } finally {
            cleanUpTxIfNeeded()
        }
    }

    override fun revert() {
        requireActiveTransaction()
        try {
            session.rollback()
            session.begin()
        } finally {
            cleanUpTxIfNeeded()
        }
    }

    private fun cleanUpTxIfNeeded() {
        if (session.status == ODatabase.STATUS.OPEN && session.transaction.status == TXSTATUS.INVALID) {
            onFinished(session, this)
        }
    }

    override fun getSnapshot(): StoreTransaction {
        return this
    }

    override fun newEntity(entityType: String): Entity {
        requireActiveWritableTransaction()
        schemaBuddy.requireTypeExists(session, entityType)
        val vertex = session.newVertex(entityType)
        session.setLocalEntityId(entityType, vertex)
        vertex.save<OVertex>()
        return OVertexEntity(vertex, store)
    }

    override fun newEntity(entityType: String, localEntityId: Long): OVertexEntity {
        requireActiveWritableTransaction()
        val vertex = session.newVertex(entityType)
        vertex.setProperty(LOCAL_ENTITY_ID_PROPERTY_NAME, localEntityId)
        vertex.save<OVertex>()
        return OVertexEntity(vertex, store)
    }

    override fun saveEntity(entity: Entity) {
        require(entity is OVertexEntity) { "Only OVertexEntity is supported, but was ${entity.javaClass.simpleName}" }
        requireActiveWritableTransaction()
        entity.save()
    }

    override fun getEntity(id: EntityId): Entity {
        requireActiveTransaction()
        val oId = store.requireOEntityId(id)
        if (oId == ORIDEntityId.EMPTY_ID) {
            throw EntityRemovedInDatabaseException(oId.getTypeName(), id)
        }
        val vertex: OVertex = session.load(oId.asOId()) ?: throw EntityRemovedInDatabaseException(oId.getTypeName(), id)
        return OVertexEntity(vertex, store)
    }

    override fun getEntityTypes(): MutableList<String> {
        requireActiveTransaction()
        return session.metadata.schema.classes.map { it.name }.toMutableList()
    }

    override fun getAll(entityType: String): EntityIterable {
        requireActiveTransaction()
        return OEntityOfTypeIterable(this, entityType)
    }

    override fun getSingletonIterable(entity: Entity): EntityIterable {
        requireActiveTransaction()
        return OSingleEntityIterable(this, entity)
    }

    override fun find(entityType: String, propertyName: String, value: Comparable<Nothing>): EntityIterable {
        requireActiveTransaction()
        return OPropertyEqualIterable(this, entityType, propertyName, value)
    }

    override fun find(
        entityType: String,
        propertyName: String,
        minValue: Comparable<Nothing>,
        maxValue: Comparable<Nothing>
    ): EntityIterable {
        requireActiveTransaction()
        return OPropertyRangeIterable(this, entityType, propertyName, minValue, maxValue)
    }

    override fun findContaining(
        entityType: String,
        propertyName: String,
        value: String,
        ignoreCase: Boolean
    ): EntityIterable {
        requireActiveTransaction()
        return OPropertyContainsIterable(this, entityType, propertyName, value)
    }

    override fun findStartingWith(entityType: String, propertyName: String, value: String): EntityIterable {
        requireActiveTransaction()
        return OPropertyStartsWithIterable(this, entityType, propertyName, value)
    }

    override fun findIds(entityType: String, minValue: Long, maxValue: Long): EntityIterable {
        requireActiveTransaction()
        return OPropertyRangeIterable(
            this,
            entityType,
            LOCAL_ENTITY_ID_PROPERTY_NAME,
            minValue,
            maxValue
        )
    }

    override fun findWithProp(entityType: String, propertyName: String): EntityIterable {
        requireActiveTransaction()
        return OPropertyExistsIterable(this, entityType, propertyName)
    }

    override fun findWithPropSortedByValue(entityType: String, propertyName: String): EntityIterable {
        requireActiveTransaction()
        return OPropertyExistsSortedIterable(this, entityType, propertyName)
    }

    override fun findWithBlob(entityType: String, blobName: String): EntityIterable {
        requireActiveTransaction()
        return OPropertyBlobExistsEntityIterable(this, entityType, blobName)
    }

    override fun findLinks(entityType: String, entity: Entity, linkName: String): EntityIterable {
        requireActiveTransaction()
        return OLinkOfTypeToEntityIterable(this, linkName, entity.id as OEntityId, entityType)
    }

    override fun findLinks(entityType: String, entities: EntityIterable, linkName: String): EntityIterable {
        requireActiveTransaction()
        return OLinkIterableToEntityIterable(this, entities.asOQueryIterable(), linkName)
    }

    override fun findWithLinks(entityType: String, linkName: String): EntityIterable {
        requireActiveTransaction()
        return OLinkExistsEntityIterable(this, entityType, linkName)
    }

    override fun findWithLinks(
        entityType: String,
        linkName: String,
        oppositeEntityType: String,
        oppositeLinkName: String
    ): EntityIterable {
        requireActiveTransaction()
        return OLinkExistsEntityIterable(this, entityType, linkName)
    }

    override fun sort(entityType: String, propertyName: String, ascending: Boolean): EntityIterable {
        requireActiveTransaction()
        return OPropertySortedIterable(this, entityType, propertyName, null, ascending)
    }

    override fun sort(
        entityType: String,
        propertyName: String,
        rightOrder: EntityIterable,
        ascending: Boolean
    ): EntityIterable {
        requireActiveTransaction()
        return OPropertySortedIterable(this, entityType, propertyName, rightOrder.asOQueryIterable(), ascending)
    }

    override fun sortLinks(
        entityType: String,
        sortedLinks: EntityIterable,
        isMultiple: Boolean,
        linkName: String,
        rightOrder: EntityIterable
    ): EntityIterable {
        requireActiveTransaction()
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
        requireActiveTransaction()
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
        requireActiveTransaction()
        val legacyId = PersistentEntityId.toEntityId(representation)
        return store.requireOEntityId(legacyId)
    }

    override fun getSequence(sequenceName: String): Sequence {
        return getSequence(sequenceName, -1)
    }

    override fun getSequence(sequenceName: String, initialValue: Long): Sequence {
        requireActiveTransaction()
        // make sure the OSequence created
        schemaBuddy.getOrCreateSequence(session, sequenceName, initialValue)
        return OSequenceImpl(sequenceName, store)
    }

    override fun getOSequence(sequenceName: String): OSequence {
        requireActiveTransaction()
        return schemaBuddy.getSequence(session, sequenceName)
    }

    override fun updateOSequence(sequenceName: String, currentValue: Long) {
        requireActiveTransaction()
        return schemaBuddy.updateSequence(session, sequenceName, currentValue)
    }

    override fun renameOClass(oldName: String, newName: String) {
        requireActiveTransaction()
        schemaBuddy.renameOClass(session, oldName, newName)
    }

    override fun getEnvironmentTransaction(): Transaction {
        requireActiveTransaction()
        return OEnvironmentTransaction(store.environment, this)
    }

    override fun setQueryCancellingPolicy(policy: QueryCancellingPolicy?) {
        require(policy is OQueryCancellingPolicy) { "Only OQueryCancellingPolicy is supported, but was ${policy?.javaClass?.simpleName}" }
        this.queryCancellingPolicy = policy
    }

    override fun getQueryCancellingPolicy() = this.queryCancellingPolicy

    override fun getOEntityId(entityId: PersistentEntityId): OEntityId {
        requireActiveTransaction()
        return schemaBuddy.getOEntityId(session, entityId)
    }

    override fun getTypeId(entityType: String): Int {
        requireActiveTransaction()
        return schemaBuddy.getTypeId(session, entityType)
    }
}
