package jetbrains.exodus.query

import java.util.*

class BoundedPriorityQueue<E>(private val capacity: Int, val comparator: Comparator<in E>) : AbstractQueue<E>() {
    private val queue = PriorityQueue(capacity, comparator)

    override val size: Int
        get() = queue.size

    override fun add(element: E) = offer(element)

    override fun offer(e: E): Boolean {
        if (queue.size >= capacity) {
            if (comparator.compare(e, queue.peek()) < 1) {
                return false
            }

            queue.poll()
        }

        val result = queue.offer(e)
        if (!result) {
            queue.offer(e)
        }
        return result
    }

    override fun poll(): E? = queue.poll()

    override fun peek(): E? = queue.peek()

    override fun iterator() = queue.iterator()
}
