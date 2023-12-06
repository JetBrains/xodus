package jetbrains.vectoriadb.index

import org.junit.Test
import java.lang.foreign.Arena

class IntSegmentTest {

    @Test
    fun `set, add, get, fill`() = Arena.ofConfined().use { arena ->
        val count = 100
        val segment = IntSegment.makeNativeSegment(arena, count)

        assert(segment.count() == count)

        repeat(count) { i ->
            segment.set(i, i + 10)
        }

        repeat(count) { i ->
            assert(segment.get(i) == i + 10)
        }

        repeat(count) { i ->
            segment.add(i, i)
        }

        repeat(count) { i ->
            assert(segment.get(i) == i + i + 10)
        }

        segment.fill(0.toByte())

        repeat(count) { i ->
            assert(segment.get(i) == 0)
        }
    }
}