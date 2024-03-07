package jetbrains.exodus.entitystore.orientdb

import jetbrains.exodus.entitystore.Entity
import jetbrains.exodus.entitystore.EntityId
import jetbrains.exodus.entitystore.iterate.EntityIterableBase
import jetbrains.exodus.entitystore.iterate.EntityIteratorBase


class OEntityIterator(
    private val iterable: EntityIterableBase,
    private val source: Iterator<Entity>
) : EntityIteratorBase(iterable) {

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
