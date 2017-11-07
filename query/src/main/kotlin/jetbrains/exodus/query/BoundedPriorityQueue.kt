package jetbrains.exodus.query

import java.util.*

class BoundedPriorityQueue<E>(private val capacity: Int, comparator: Comparator<in E>) : AbstractQueue<E>() {
    private val queue = PriorityQueue(capacity, comparator)
    private val comparator = Collections.reverseOrder(comparator)

    override val size: Int
        get() = queue.size

    override fun add(element: E) = offer(element)

    override fun offer(e: E): Boolean {
        if (queue.size >= capacity) {
            if (comparator.compare(e, peek()) < 1) {
                return false
            }

            poll()
        }

        return queue.offer(e)
    }

    override fun poll(): E? = queue.poll()

    override fun peek(): E? = queue.peek()

    override fun iterator() = queue.iterator() // TODO: reverse?
}
