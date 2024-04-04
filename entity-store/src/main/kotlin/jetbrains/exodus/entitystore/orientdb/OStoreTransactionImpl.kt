package jetbrains.exodus.entitystore.orientdb

import com.orientechnologies.orient.core.db.document.ODatabaseDocument
import com.orientechnologies.orient.core.db.record.ORecordOperation
import com.orientechnologies.orient.core.record.OVertex
import com.orientechnologies.orient.core.tx.OTransaction
import com.orientechnologies.orient.core.tx.OTransactionNoTx
import jetbrains.exodus.entitystore.*
import jetbrains.exodus.entitystore.iterate.EntityIterableBase
import jetbrains.exodus.entitystore.iterate.property.*
import jetbrains.exodus.entitystore.orientdb.iterate.OEntityIterableBase
import jetbrains.exodus.entitystore.orientdb.iterate.OEntityOfTypeIterable
import jetbrains.exodus.entitystore.orientdb.iterate.link.OLinkExistsEntityIterable
import jetbrains.exodus.entitystore.orientdb.iterate.link.OLinkSortEntityIterable
import jetbrains.exodus.entitystore.orientdb.iterate.link.OLinkToEntityIterable
import jetbrains.exodus.entitystore.orientdb.iterate.property.OSequenceImpl
import jetbrains.exodus.env.Transaction

class OStoreTransactionImpl(
    private val session: ODatabaseDocument,
    private val txn: OTransaction,
    private val store: PersistentEntityStore
) : OStoreTransaction {

    private var queryCancellingPolicy:QueryCancellingPolicy? = null

    override val oTransaction = txn

    override fun activeSession(): ODatabaseDocument {
        return session
    }

    override fun getStore(): EntityStore {
        return store
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

    override fun isCurrent(): Boolean {
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
        return OVertexEntity(session.newVertex(entityType), store)
    }

    override fun saveEntity(entity: Entity) {
        require(entity is OVertexEntity) { "Only OVertexEntity is supported, but was ${entity.javaClass.simpleName}" }
        entity.save()
    }

    override fun getEntity(id: EntityId): Entity {
        require(id is OEntityId) { "Only OEntity is supported, but was ${id.javaClass.simpleName}" }
        val vertex: OVertex = session.load(id.asOId())
        return OVertexEntity(vertex, store)
    }

    override fun getEntityTypes(): MutableList<String> {
        return session.metadata.schema.classes.map { it.name }.toMutableList()
    }

    override fun getAll(entityType: String): EntityIterable {
        return OEntityOfTypeIterable(this, entityType)
    }

    override fun getSingletonIterable(entity: Entity): EntityIterable {
        TODO("Not Implemented")
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
        TODO("Not yet implemented")
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
        return OLinkToEntityIterable(this, linkName, entity.id as OEntityId)
    }

    override fun findLinks(entityType: String, entities: EntityIterable, linkName: String): EntityIterable {
        var links: MutableList<EntityIterable>? = null
        for (entity in entities) {
            if (links == null) {
                links = ArrayList()
            }
            links.add(findLinks(entityType, entity, linkName))
        }
        if (links == null) {
            // ToDo: return OEntityIterableBase.EMPTY
            return EntityIterableBase.EMPTY
        }
        if (links.size > 1) {
            var i = 0
            while (i < links.size - 1) {
                links.add(links[i].union(links[i + 1]))
                i += 2
            }
        }
        return links[links.size - 1]
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
        return OPropertySortedIterable(this, entityType, propertyName, rightOrder.asOIterable(), ascending)
    }

    override fun sortLinks(
        entityType: String,
        sortedLinks: EntityIterable,
        isMultiple: Boolean,
        linkName: String,
        rightOrder: EntityIterable
    ): EntityIterable {
        return OLinkSortEntityIterable(this, sortedLinks.asOIterable(), linkName, rightOrder.asOIterable())
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

    override fun mergeSorted(
        sorted: List<EntityIterable?>,
        valueGetter: ComparableGetter,
        comparator: java.util.Comparator<Comparable<Any>?>
    ): EntityIterable {
        TODO("Not yet implemented")
    }

    override fun toEntityId(representation: String): EntityId {
        TODO("Not yet implemented")
    }

    override fun getSequence(sequenceName: String): Sequence {
        return OSequenceImpl(sequenceName)
    }

    override fun getSequence(sequenceName: String, initialValue: Long): Sequence {
        return OSequenceImpl(sequenceName, initialValue)
    }

    override fun getEnvironmentTransaction(): Transaction {
        return OEnvironmentTransaction(store.environment, this)
    }

    override fun setQueryCancellingPolicy(policy: QueryCancellingPolicy?) {
        this.queryCancellingPolicy = policy
    }

    override fun getQueryCancellingPolicy() = this.queryCancellingPolicy


    private fun EntityIterable.asOIterable(): OEntityIterableBase {
        require(this is OEntityIterableBase) { "Only OEntityIterableBase is supported, but was ${this.javaClass.simpleName}" }
        return this
    }
}
