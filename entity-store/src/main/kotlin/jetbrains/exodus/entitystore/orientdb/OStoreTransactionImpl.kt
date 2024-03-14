package jetbrains.exodus.entitystore.orientdb

import com.orientechnologies.orient.core.db.document.ODatabaseDocument
import com.orientechnologies.orient.core.db.record.ORecordOperation
import com.orientechnologies.orient.core.record.OVertex
import com.orientechnologies.orient.core.tx.OTransaction
import com.orientechnologies.orient.core.tx.OTransactionNoTx
import jetbrains.exodus.entitystore.*
import jetbrains.exodus.env.Transaction

class OStoreTransactionImpl(
    private val session: ODatabaseDocument,
    private val txn: OTransaction,
    private val oStore: OPersistentStore
) : OStoreTransaction {
    override fun activeSession(): ODatabaseDocument {
        return session
    }

    override fun getStore(): EntityStore {
        return oStore
    }

    override fun isIdempotent(): Boolean {
        val operations = txn.recordOperations ?: listOf()
        return operations.firstOrNull() == null || operations.all { it.type == ORecordOperation.LOADED }
    }

    override fun isReadonly(): Boolean {
        return txn is OTransactionNoTx
    }

    override fun isFinished(): Boolean {
        return !txn.isActive
    }

    override fun commit(): Boolean {
        txn.commit()
        return true
    }

    override fun abort() {
        txn.rollback()
    }

    override fun flush(): Boolean {
        commit()
        txn.begin()
        return true
    }

    override fun revert() {
        txn.rollback()
    }

    override fun getSnapshot(): StoreTransaction {
        return this
    }

    override fun newEntity(entityType: String): Entity {
        return OVertexEntity(session.newVertex(entityType))
    }

    override fun saveEntity(entity: Entity) {
        require(entity is OVertexEntity) { "Only OVertexEntity is supported, but was ${entity.javaClass.simpleName}" }
        (entity as? OVertexEntity) ?: throw IllegalArgumentException("Only OVertexEntity supported")
        entity.save()

    }

    override fun getEntity(id: EntityId): Entity {
        require(id is OEntityId) { "Only OEntity is supported, but was ${id.javaClass.simpleName}" }
        val vertex: OVertex = session.load(id.asOId())
        return OVertexEntity(vertex)
    }

    override fun getEntityTypes(): MutableList<String> {
        return session.metadata.schema.classes.map { it.name }.toMutableList()
    }

    override fun getAll(entityType: String): EntityIterable {
        TODO("Not Implemented")
    }

    override fun getSingletonIterable(entity: Entity): EntityIterable {
        TODO("Not Implemented")
    }

    override fun find(entityType: String, propertyName: String, value: Comparable<Nothing>): EntityIterable {
        TODO("Not yet implemented")
    }

    override fun find(
        entityType: String,
        propertyName: String,
        minValue: Comparable<Nothing>,
        maxValue: Comparable<Nothing>
    ): EntityIterable {
        TODO("Not yet implemented")
    }

    override fun findContaining(
        entityType: String,
        propertyName: String,
        value: String,
        ignoreCase: Boolean
    ): EntityIterable {
        TODO("Not yet implemented")
    }

    override fun findStartingWith(entityType: String, propertyName: String, value: String): EntityIterable {
        TODO("Not yet implemented")
    }

    override fun findIds(entityType: String, minValue: Long, maxValue: Long): EntityIterable {
        TODO("Not yet implemented")
    }

    override fun findWithProp(entityType: String, propertyName: String): EntityIterable {
        TODO("Not yet implemented")
    }

    override fun findWithPropSortedByValue(entityType: String, propertyName: String): EntityIterable {
        TODO("Not yet implemented")
    }

    override fun findWithBlob(entityType: String, blobName: String): EntityIterable {
        TODO("Not yet implemented")
    }

    override fun findLinks(entityType: String, entity: Entity, linkName: String): EntityIterable {
        TODO("Not yet implemented")
    }

    override fun findLinks(entityType: String, entities: EntityIterable, linkName: String): EntityIterable {
        TODO("Not yet implemented")
    }

    override fun findWithLinks(entityType: String, linkName: String): EntityIterable {
        TODO("Not yet implemented")
    }

    override fun findWithLinks(
        entityType: String,
        linkName: String,
        oppositeEntityType: String,
        oppositeLinkName: String
    ): EntityIterable {
        TODO("Not yet implemented")
    }

    override fun sort(entityType: String, propertyName: String, ascending: Boolean): EntityIterable {
        TODO("Not yet implemented")
    }

    override fun sort(
        entityType: String,
        propertyName: String,
        rightOrder: EntityIterable,
        ascending: Boolean
    ): EntityIterable {
        TODO("Not yet implemented")
    }

    override fun sortLinks(
        entityType: String,
        sortedLinks: EntityIterable,
        isMultiple: Boolean,
        linkName: String,
        rightOrder: EntityIterable
    ): EntityIterable {
        TODO("Not yet implemented")
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
        TODO("Not yet implemented")
    }

    override fun mergeSorted(sorted: MutableList<EntityIterable>, comparator: Comparator<Entity>): EntityIterable {
        TODO("Not yet implemented")
    }

    override fun toEntityId(representation: String): EntityId {
        TODO("Not yet implemented")
    }

    override fun getSequence(sequenceName: String): Sequence {
        TODO("Not yet implemented")
    }

    override fun getSequence(sequenceName: String, initialValue: Long): Sequence {
        TODO("Not yet implemented")
    }

    override fun setQueryCancellingPolicy(policy: QueryCancellingPolicy?) {
        TODO("Not yet implemented")
    }

    override fun getQueryCancellingPolicy(): QueryCancellingPolicy? {
        TODO("Not yet implemented")
    }

    override fun getEnvironmentTransaction(): Transaction {
        return OEnvironmentTransaction(oStore.environment, this)
    }
}
