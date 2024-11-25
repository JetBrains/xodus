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
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal
import com.orientechnologies.orient.core.exception.ORecordNotFoundException
import com.orientechnologies.orient.core.metadata.schema.OClass
import com.orientechnologies.orient.core.metadata.sequence.OSequence
import com.orientechnologies.orient.core.record.ORecord
import com.orientechnologies.orient.core.record.OVertex
import com.orientechnologies.orient.core.sql.executor.OResultSet
import com.orientechnologies.orient.core.tx.OTransaction.TXSTATUS
import jetbrains.exodus.entitystore.*
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.LOCAL_ENTITY_ID_PROPERTY_NAME
import jetbrains.exodus.entitystore.orientdb.iterate.OEntityOfTypeIterable
import jetbrains.exodus.entitystore.orientdb.iterate.link.*
import jetbrains.exodus.entitystore.orientdb.iterate.property.*
import jetbrains.exodus.entitystore.orientdb.query.OQueryCancellingPolicy

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
        (session as ODatabaseSessionInternal).transaction.id.toLong()
    }

    private val resultSets: MutableCollection<OResultSet> = arrayListOf()

    override fun getTransactionId(): Long {
        return transactionIdImpl
    }

    override fun <T> getRecord(id: OEntityId): T
            where T : ORecord {
        requireActiveTransaction()
        try {
            return session.load(id.asOId())
        } catch (e: ORecordNotFoundException) {
            throw EntityRemovedInDatabaseException(id.getTypeName(), id)
        }

    }

    override fun query(sql: String, params: Map<String, Any>): OResultSet {
        requireActiveTransaction()
        return session.query(sql, params)
    }

    override fun getStore(): EntityStore {
        return store
    }

    override fun isIdempotent(): Boolean {
        return readOnly || (session as ODatabaseSessionInternal).transaction.recordOperations.none()
    }

    override fun isReadonly(): Boolean {
        return readOnly
    }

    override fun isFinished(): Boolean {
        return session.status == ODatabaseSession.STATUS.CLOSED
    }

    override fun isCurrent(): Boolean {
        return !isFinished && session.isActiveOnCurrentThread
    }

    override fun getOEntityStore(): OEntityStore {
        return store
    }

    override fun requireActiveTransaction() {
        check(session.status == ODatabaseSession.STATUS.OPEN) { "The transaction is finished, the internal session state: ${session.status}" }
        check(session.isActiveOnCurrentThread) { "The active session is no the session the transaction was started in" }
        check((session as ODatabaseSessionInternal).transaction.status == TXSTATUS.BEGUN) { "The current OTransaction status is ${session.transaction.status}, but the status ${TXSTATUS.BEGUN} was expected." }
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
        check(session.status == ODatabaseSession.STATUS.OPEN) { "The transaction is finished, the internal session state: ${session.status}" }
        check(!session.isActiveOnCurrentThread) { "The transaction is already active on the current thread" }
        onActivated(session, this)
    }

    fun begin() {
        check(session.status == ODatabaseSession.STATUS.OPEN) { "The session status is ${session.status}. But ${ODatabaseSession.STATUS.OPEN} is required." }
        check(session.isActiveOnCurrentThread) { "The session is not active on the current thread" }
        check(session.activeTxCount() == 0) { "The session must not have a transaction" }
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

    override fun bindToSession(vertex: OVertex): OVertex {
        requireActiveTransaction()

        return session.bindToSession(vertex)
    }

    override fun bindToSession(entity: OVertexEntity): OVertexEntity {
        requireActiveTransaction()

        if (entity.isUnloaded) {
            return OVertexEntity(bindToSession(entity.vertex), store)
        }

        return entity
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
        if (session.status == ODatabaseSession.STATUS.OPEN && session.activeTxCount() == 0) {
            resultSets.forEach(OResultSet::close)
            resultSets.clear()
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
        schemaBuddy.requireTypeExists(session, entityType)
        val vertex = session.newVertex(entityType)
        vertex.setProperty(LOCAL_ENTITY_ID_PROPERTY_NAME, localEntityId)
        vertex.save<OVertex>()
        return OVertexEntity(vertex, store)
    }

    override fun generateEntityId(entityType: String, vertex: OVertex) {
        session.setLocalEntityId(entityType, vertex)
        vertex.save<OVertex>()
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
        try {
            val vertex: OVertex = session.load(oId.asOId())
            return OVertexEntity(vertex, store)
        } catch (e: ORecordNotFoundException) {
            throw EntityRemovedInDatabaseException(oId.getTypeName(), id)
        }
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

    override fun find(
        entityType: String,
        propertyName: String,
        value: Comparable<Nothing>
    ): EntityIterable {
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
        return OPropertyContainsIterable(this, entityType, propertyName, value, ignoreCase)
    }

    override fun findStartingWith(
        entityType: String,
        propertyName: String,
        value: String
    ): EntityIterable {
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

    override fun findWithPropSortedByValue(
        entityType: String,
        propertyName: String
    ): EntityIterable {
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

    override fun findLinks(
        entityType: String,
        entities: EntityIterable,
        linkName: String
    ): EntityIterable {
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

    override fun sort(
        entityType: String,
        propertyName: String,
        ascending: Boolean
    ): EntityIterable {
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
        return OPropertySortedIterable(
            this,
            entityType,
            propertyName,
            rightOrder.asOQueryIterable(),
            ascending
        )
    }

    override fun sortLinks(
        entityType: String,
        sortedLinks: EntityIterable,
        isMultiple: Boolean,
        linkName: String,
        rightOrder: EntityIterable
    ): EntityIterable {
        requireActiveTransaction()
        return OLinkSortEntityIterable(
            this,
            sortedLinks.asOQueryIterable(),
            linkName,
            rightOrder.asOQueryIterable()
        )
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
        return OLinkSortEntityIterable(
            this,
            sortedLinks.asOQueryIterable(),
            linkName,
            rightOrder.asOQueryIterable()
        )
    }

    @Deprecated("Deprecated in Java")
    override fun mergeSorted(
        sorted: MutableList<EntityIterable>,
        comparator: Comparator<Entity>
    ): EntityIterable {
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

    override fun getOrCreateEdgeClass(
        linkName: String,
        outClassName: String,
        inClassName: String
    ): OClass {
        requireActiveTransaction()
        return schemaBuddy.getOrCreateEdgeClass(session, linkName, outClassName, inClassName)
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

    override fun getType(entityTypeId: Int): String {
        requireActiveTransaction()
        return schemaBuddy.getType(session, entityTypeId)
    }

    override fun bindResultSet(resultSet: OResultSet) {
        resultSets.add(resultSet)
    }

}
