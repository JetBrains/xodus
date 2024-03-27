package jetbrains.exodus.entitystore.orientdb.iterate

import jetbrains.exodus.entitystore.Entity
import jetbrains.exodus.entitystore.EntityId
import jetbrains.exodus.entitystore.EntityIterator
import jetbrains.exodus.entitystore.PersistentStoreTransaction
import jetbrains.exodus.entitystore.orientdb.query.OQuery
import jetbrains.exodus.entitystore.orientdb.toEntityIterator
import mu.KLogging


class OQueryEntityIterator(private val source: Iterator<Entity>) : EntityIterator {

    companion object : KLogging() {

        fun create(query: OQuery, txn: PersistentStoreTransaction): OQueryEntityIterator {
            val resultSet = query.execute()
            // Log execution plan
            val executionPlan = resultSet.executionPlan.get().prettyPrint(10, 8)
            logger.info { "Query: ${query.sql()} with params: ${query.params()}, \n execution plan:\n  $executionPlan, \n stats: ${resultSet.queryStats}" }

            val iterator = resultSet.toEntityIterator(txn.store)
            return OQueryEntityIterator(iterator)
        }
    }

    override fun next(): Entity? {
        return source.next()
    }

    override fun hasNext(): Boolean {
        return source.hasNext()
    }

    override fun skip(number: Int): Boolean {
        repeat(number) {
            if (!hasNext()) {
                return false
            }
            next()
        }
        return true
    }

    override fun nextId(): EntityId? {
        return next()?.id
    }

    override fun dispose(): Boolean {
        return true
    }

    override fun shouldBeDisposed(): Boolean {
        return false
    }

    override fun remove() {
        throw UnsupportedOperationException()
    }
}
