package jetbrains.exodus.query

import jetbrains.exodus.entitystore.Entity
import java.util.*

class InMemoryBoundedHeapSortIterable(val capacity: Int, source: Iterable<Entity>, comparator: Comparator<Entity>) : InMemoryQueueSortIterable(source, comparator) {

    override fun createQueue(unsorted: Collection<Entity>): Queue<Entity> {
        val result = BoundedPriorityQueue(capacity, comparator)
        unsorted.forEach { result.offer(it) }
        return result
    }
}
