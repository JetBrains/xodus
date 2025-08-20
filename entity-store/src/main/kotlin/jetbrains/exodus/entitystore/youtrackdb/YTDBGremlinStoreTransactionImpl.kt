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
import com.jetbrains.youtrack.db.api.common.BasicDatabaseSession.STATUS
import com.jetbrains.youtrack.db.api.exception.ModificationOperationProhibitedException
import com.jetbrains.youtrack.db.api.exception.RecordNotFoundException
import com.jetbrains.youtrack.db.api.query.ResultSet
import com.jetbrains.youtrack.db.api.record.DBRecord
import com.jetbrains.youtrack.db.api.record.Vertex
import com.jetbrains.youtrack.db.api.schema.SchemaClass
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal
import com.jetbrains.youtrack.db.internal.core.id.ImmutableRecordId
import com.jetbrains.youtrack.db.internal.core.metadata.sequence.DBSequence
import com.jetbrains.youtrack.db.internal.core.tx.FrontendTransaction
import jetbrains.exodus.entitystore.*
import jetbrains.exodus.entitystore.youtrackdb.YTDBVertexEntity.Companion.LOCAL_ENTITY_ID_PROPERTY_NAME
import jetbrains.exodus.entitystore.youtrackdb.gremlin.GremlinBlock
import jetbrains.exodus.entitystore.youtrackdb.gremlin.GremlinBlock.SortDirection
import jetbrains.exodus.entitystore.youtrackdb.gremlin.GremlinEntityIterable
import jetbrains.exodus.entitystore.youtrackdb.gremlin.GremlinEntityIterableImpl
import jetbrains.exodus.entitystore.youtrackdb.gremlin.GremlinQuery
import jetbrains.exodus.entitystore.youtrackdb.iterate.property.YTDBSequenceImpl
import jetbrains.exodus.entitystore.youtrackdb.query.YTDBQueryCancellingPolicy
import jetbrains.exodus.env.ReadonlyTransactionException
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource

internal typealias TransactionEventHandler = (DatabaseSession, YTDBStoreTransaction) -> Unit

class YTDBGremlinStoreTransactionImpl(
    private val session: DatabaseSession,
    private val store: YTDBPersistentEntityStore,
    private val schemaBuddy: YTDBSchemaBuddy,
    private val onFinished: TransactionEventHandler,
    private val onDeactivated: TransactionEventHandler,
    private val onActivated: TransactionEventHandler,
    private val readOnly: Boolean = false
) : YTDBStoreTransaction {
    private var queryCancellingPolicy: YTDBQueryCancellingPolicy? = null

    /**
     * The Orient transaction gets changed on flush(), so its id gets changed too.
     * It would be strange if id of OStoreTransaction gets changed during its lifetime,
     * so it was decided to remember the first Orient transaction id and use it as OStoreTransaction id.
     *
     * If you think that it should be implemented differently, come and let's discuss.
     */
    private val transactionIdImpl by lazy {
        requireActiveTransaction()
        (session as DatabaseSessionInternal).activeTransaction.id
    }

    private val resultSets: MutableCollection<ResultSet> = arrayListOf()

    override fun getTransactionId(): Long {
        return transactionIdImpl
    }

    override fun <T> getRecord(id: YTDBEntityId): T
            where T : DBRecord {
        try {
            return requireActiveTransaction().load(id.asOId())
        } catch (_: RecordNotFoundException) {
            throw EntityRemovedInDatabaseException(id.getTypeName(), id)
        }

    }

    @Deprecated("should be removed")
    override fun query(sql: String, params: Map<String, Any>): ResultSet {
        return requireActiveTransaction().query(sql, params)
    }

    override fun g(): GraphTraversalSource {
        return requireActiveTransaction().traversal();
    }

    override fun getStore(): EntityStore {
        return store
    }

    override fun isIdempotent(): Boolean {
        return readOnly || (session as DatabaseSessionInternal).activeTransaction.recordOperations.findAny().isEmpty
    }

    override fun isReadonly(): Boolean {
        return readOnly
    }

    override fun isFinished(): Boolean {
        return session.isClosed
    }

    override fun isCurrent(): Boolean {
        return !isFinished && (session as DatabaseSessionInternal).isActiveOnCurrentThread
    }

    override val databaseSession: DatabaseSession
        get() = session

    override fun getOEntityStore(): YTDBEntityStore {
        return store
    }

    override fun requireActiveTransaction(): FrontendTransaction {
        check(session.status == STATUS.OPEN) {
            "The transaction is finished, the internal session state: ${session.status}"
        }
        check((session as DatabaseSessionInternal).isActiveOnCurrentThread) {
            "The active session is no the session the transaction was started in"
        }
        val currentTx = session.activeTransaction
        check(currentTx.status == FrontendTransaction.TXSTATUS.BEGUN) {
            "The current OTransaction status is ${currentTx.status}, but the status ${FrontendTransaction.TXSTATUS.BEGUN} was expected."
        }
        return currentTx;
    }

    override fun requireActiveWritableTransaction(): FrontendTransaction {
        check(!readOnly) { "Cannot modify read-only transaction" }
        return requireActiveTransaction()
    }

    override fun deactivateOnCurrentThread() {
        requireActiveTransaction()
        onDeactivated(session, this)
    }

    override fun activateOnCurrentThread() {
        check(session.status == STATUS.OPEN) { "The transaction is finished, the internal session state: ${session.status}" }
        onActivated(session, this)
    }

    fun begin() {
        check(session.status == STATUS.OPEN) { "The session status is ${session.status}. But ${STATUS.OPEN} is required." }
        check((session as DatabaseSessionInternal).isActiveOnCurrentThread) { "The session is not active on the current thread" }
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
        try {
            requireActiveTransaction().commit()
        } finally {
            cleanUpTxIfNeeded()
        }

        return true
    }

    override fun flush(): Boolean {
        try {
            requireActiveTransaction().commit()
            session.begin()
        } catch (_: ModificationOperationProhibitedException) {
            throw ReadonlyTransactionException()
        } finally {
            cleanUpTxIfNeeded()
        }

        return true
    }

    override fun abort() {
        try {
            requireActiveTransaction().rollback()
        } finally {
            cleanUpTxIfNeeded()
        }
    }

    override fun bindToSession(vertex: Vertex): Vertex {
        return requireActiveTransaction().loadVertex(vertex)
    }

    override fun bindToSession(entity: YTDBVertexEntity): YTDBVertexEntity {
        requireActiveTransaction()

        if (entity.isUnloaded) {
            return YTDBVertexEntity(bindToSession(entity.vertex), store)
        }

        return entity
    }

    override fun revert() {
        try {
            requireActiveTransaction().rollback()
            session.begin()
        } finally {
            cleanUpTxIfNeeded()
        }
    }

    private fun cleanUpTxIfNeeded() {
        if (session.status == STATUS.OPEN && session.activeTxCount() == 0) {
            resultSets.forEach(ResultSet::close)
            resultSets.clear()
            onFinished(session, this)
        }
    }

    override fun getSnapshot(): StoreTransaction {
        return this
    }

    override fun newEntity(entityType: String): Entity {
        schemaBuddy.requireTypeExists(session, entityType)
        val vertex = requireActiveWritableTransaction().newVertex(entityType)
        session.setLocalEntityId(entityType, vertex)
        return YTDBVertexEntity(vertex, store)
    }

    override fun newEntity(entityType: String, localEntityId: Long): YTDBVertexEntity {
        schemaBuddy.requireTypeExists(session, entityType)
        val vertex = requireActiveWritableTransaction().newVertex(entityType)
        vertex.setProperty(LOCAL_ENTITY_ID_PROPERTY_NAME, localEntityId)
        return YTDBVertexEntity(vertex, store)
    }

    override fun generateEntityId(entityType: String, vertex: Vertex) {
        session.setLocalEntityId(entityType, vertex)
    }

    override fun saveEntity(entity: Entity) {
        require(entity is YTDBVertexEntity) { "Only OVertexEntity is supported, but was ${entity.javaClass.simpleName}" }
        requireActiveWritableTransaction()
    }

    override fun getEntity(id: EntityId): Entity {
        val tx = requireActiveTransaction()
        val oId = store.requireOEntityId(id)
        val ytdbId = oId.asOId()
        if (ytdbId == ImmutableRecordId.EMPTY_RECORD_ID) {
            throw EntityRemovedInDatabaseException(oId.getTypeName(), id)
        }
        try {
            val vertex: Vertex = tx.load(ytdbId)
            return YTDBVertexEntity(vertex, store)
        } catch (_: RecordNotFoundException) {
            throw EntityRemovedInDatabaseException(oId.getTypeName(), id)
        }
    }

    override fun getEntityTypes(): List<String> {
        requireActiveTransaction()
        return session.schema.classes
            .filter { it.isVertexType && it.name != Vertex.CLASS_NAME }
            .map { it.name }
    }

    override fun getAll(entityType: String): EntityIterable {
        requireActiveTransaction()
        return GremlinEntityIterable.where(entityType, this, GremlinBlock.All)
    }

    override fun getSingletonIterable(entity: Entity): EntityIterable {
        requireActiveTransaction()
        return GremlinEntityIterable.query(
            this, GremlinQuery.all
                .then(GremlinBlock.IdEqual((entity.id as YTDBEntityId).asOId()))
        )
    }

    override fun find(
        entityType: String,
        propertyName: String,
        value: Comparable<Nothing>
    ): EntityIterable {
        requireActiveTransaction()
        return GremlinEntityIterable.where(entityType, this, GremlinBlock.PropEqual(propertyName, value))
    }

    override fun find(
        entityType: String,
        propertyName: String,
        minValue: Comparable<Nothing>,
        maxValue: Comparable<Nothing>
    ): EntityIterable {
        requireActiveTransaction()
        return GremlinEntityIterable.where(
            entityType,
            this,
            GremlinBlock.PropInRange(propertyName, minValue, maxValue)
        )
    }

    override fun findContaining(
        entityType: String,
        propertyName: String,
        value: String,
        ignoreCase: Boolean
    ): EntityIterable {
        requireActiveTransaction()
        return GremlinEntityIterable.where(
            entityType,
            this,
            GremlinBlock.HasSubstring(propertyName, value, !ignoreCase)
        )
    }

    override fun findStartingWith(
        entityType: String,
        propertyName: String,
        value: String
    ): EntityIterable {
        requireActiveTransaction()
        return GremlinEntityIterable.where(entityType, this, GremlinBlock.HasPrefix(propertyName, value, false))
    }

    override fun findIds(entityType: String, minValue: Long, maxValue: Long): EntityIterable {
        requireActiveTransaction()
        return GremlinEntityIterable.where(
            entityType,
            this,
            GremlinBlock.PropInRange(LOCAL_ENTITY_ID_PROPERTY_NAME, minValue, maxValue)
        )
    }

    override fun findWithProp(entityType: String, propertyName: String): EntityIterable {
        requireActiveTransaction()
        return GremlinEntityIterable.where(entityType, this, GremlinBlock.PropNotNull(propertyName))
    }

    override fun findWithPropSortedByValue(
        entityType: String,
        propertyName: String
    ): EntityIterable {
        requireActiveTransaction()
        return GremlinEntityIterable.query(
            this,
            GremlinQuery.all
                .then(GremlinBlock.HasLabel(entityType))
                .then(GremlinBlock.PropNotNull(propertyName))
                // todo: move SortBy out of Condition
                .then(GremlinBlock.Sort(GremlinBlock.Sort.ByProp(propertyName), SortDirection.ASC))
        )
    }

    override fun findWithBlob(entityType: String, blobName: String): EntityIterable {
        return findWithProp(entityType, blobName)
    }

    override fun findLinks(entityType: String, entity: Entity, linkName: String): EntityIterable {
        requireActiveTransaction()

        return GremlinEntityIterable.query(
            this,
            GremlinQuery.all
                .then(GremlinBlock.PropEqual(LOCAL_ENTITY_ID_PROPERTY_NAME, entity.id.localId))
                .then(GremlinBlock.InLink(linkName))
                .then(GremlinBlock.HasLabel(entityType))
        )
    }

    override fun findLinks(
        entityType: String,
        entities: EntityIterable,
        linkName: String
    ): EntityIterable {
        requireActiveTransaction()
        val entityQuery = entities.asGremlinIterable().query
        return GremlinEntityIterable.query(
            this,
            entityQuery
                .then(GremlinBlock.InLink(linkName))
                .then(GremlinBlock.HasLabel(entityType))
        )
    }

    override fun findWithLinks(entityType: String, linkName: String): EntityIterable {
        requireActiveTransaction()
        return GremlinEntityIterable.where(entityType, this, GremlinBlock.HasLink(linkName))
    }

    override fun findWithLinks(
        entityType: String,
        linkName: String,
        oppositeEntityType: String,
        oppositeLinkName: String
    ): EntityIterable {
        requireActiveTransaction()
        return GremlinEntityIterable.where(entityType, this, GremlinBlock.HasLink(linkName))
    }

    override fun sort(
        entityType: String,
        propertyName: String,
        ascending: Boolean
    ): EntityIterable {
        requireActiveTransaction()
        return GremlinEntityIterable.query(
            this,
            GremlinQuery.all
                .then(GremlinBlock.HasLabel(entityType))
                .then(
                    GremlinBlock.Sort(
                        GremlinBlock.Sort.ByProp(propertyName),
                        if (ascending) SortDirection.ASC else SortDirection.DESC
                    )
                )
        )
    }

    override fun sort(
        entityType: String,
        propertyName: String,
        rightOrder: EntityIterable,
        ascending: Boolean
    ): EntityIterable {
        requireActiveTransaction()
        return GremlinEntityIterableImpl(
            this,
            rightOrder.asGremlinIterable().query
                .then(
                    GremlinBlock.Sort(
                        GremlinBlock.Sort.ByProp(propertyName),
                        if (ascending) SortDirection.ASC else SortDirection.DESC
                    )
                )
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
        return GremlinEntityIterable.query(
            this,
            sortedLinks.asGremlinIterable().query
                .then(GremlinBlock.InLink(linkName))
                .intersect(rightOrder.asGremlinIterable().query)
                .then(GremlinBlock.HasLabel(entityType))
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
        // todo: check if we need this
        // Not sure about skipping oppositeEntityType and oppositeLinkName values
        return GremlinEntityIterable.query(
            this,
            sortedLinks.asGremlinIterable().query
                .then(GremlinBlock.InLink(linkName))
                .intersect(rightOrder.asGremlinIterable().query)
                .then(GremlinBlock.HasLabel(entityType))
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
        val oEntityId = store.requireOEntityId(legacyId)
        return if (oEntityId == RIDEntityId.EMPTY_ID) {
            RIDEntityId(
                legacyId.typeId, legacyId.localId,
                ImmutableRecordId.EMPTY_RECORD_ID, null
            )
        } else {
            oEntityId
        }
    }

    override fun getSequence(sequenceName: String): Sequence {
        return getSequence(sequenceName, -1)
    }

    override fun getSequence(sequenceName: String, initialValue: Long): Sequence {
        requireActiveTransaction()
        // make sure the OSequence created
        schemaBuddy.getOrCreateSequence(session, sequenceName, initialValue)
        return YTDBSequenceImpl(session as DatabaseSessionInternal, sequenceName, store)
    }

    override fun getOSequence(sequenceName: String): DBSequence {
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

    override fun deleteOClass(entityTypeName: String) {
        requireActiveTransaction()
        schemaBuddy.deleteOClass(session, entityTypeName)
    }

    override fun getOrCreateEdgeClass(
        linkName: String,
        outClassName: String,
        inClassName: String
    ): SchemaClass {
        requireActiveTransaction()
        return schemaBuddy.getOrCreateEdgeClass(session, linkName, outClassName, inClassName)
    }

    override fun setQueryCancellingPolicy(policy: QueryCancellingPolicy?) {
        require(policy is YTDBQueryCancellingPolicy) { "Only OQueryCancellingPolicy is supported, but was ${policy?.javaClass?.simpleName}" }
        this.queryCancellingPolicy = policy
    }

    override fun getQueryCancellingPolicy() = this.queryCancellingPolicy

    override fun getOEntityId(entityId: PersistentEntityId): YTDBEntityId {
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


    override fun bindResultSet(resultSet: ResultSet) {
        resultSets.add(resultSet)
    }

}
