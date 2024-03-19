package jetbrains.exodus.entitystore.orientdb

import com.orientechnologies.orient.core.db.document.ODatabaseDocument
import jetbrains.exodus.entitystore.Entity
import jetbrains.exodus.entitystore.EntityId
import jetbrains.exodus.entitystore.iterate.EntityIterableBase
import jetbrains.exodus.entitystore.iterate.EntityIteratorBase
import jetbrains.exodus.entitystore.iterate.OEntityIterableBase
import mu.KLogging


class OQueryEntityIterator(
    private val iterable: EntityIterableBase,
    private val source: Iterator<Entity>
) : EntityIteratorBase(iterable) {

    companion object : KLogging() {

        fun create(iterable: OEntityIterableBase, document: ODatabaseDocument, query: OQuery): OQueryEntityIterator {
            val resultSet = document.query(query.sql(), *query.params().toTypedArray())
            // Log execution plan
            val executionPlan = resultSet.executionPlan.get().prettyPrint(10, 8)
            logger.info { "Query: ${query.sql()} with params: ${query.params()}, execution plan:\n  $executionPlan" }

            val iterator = resultSet.toEntityIterator()
            return OQueryEntityIterator(iterable, iterator)
        }
    }

    override fun next(): Entity? {
        return source.next()
    }

    override fun hasNextImpl(): Boolean {
        return source.hasNext()
    }

    override fun nextIdImpl(): EntityId? {
        return next()?.id
    }
}