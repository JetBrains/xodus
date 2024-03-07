package jetbrains.exodus.entitystore.orientdb

import jetbrains.exodus.entitystore.Entity
import jetbrains.exodus.entitystore.EntityId
import jetbrains.exodus.entitystore.EntityIterator


class OEntityIterator(
    private val iterator: Iterator<Entity>
) : EntityIterator, Iterator<Entity> by iterator {

    override fun remove() {
        throw UnsupportedOperationException()
    }

    override fun skip(number: Int): Boolean {
        for (i in 0 until number) {
            if (!hasNext()) {
                return false
            }
            next()
        }
        return true
    }

    override fun nextId(): EntityId? {
        return next().id
    }

    override fun dispose(): Boolean {
        return false
    }

    override fun shouldBeDisposed(): Boolean {
        return false
    }
}
