package jetbrains.vectoriadb.index.segment

import org.junit.Test
import java.lang.foreign.Arena

class ByteCodeSegmentTest {

    @Test
    fun `simple set, get`() = Arena.ofConfined().use { arena ->
        val count = 256
        val segment = ByteCodeSegment.makeNativeSegment(arena, count)

        assert(segment.maxNumberOfCodes() == count)
        assert(segment.count() == count)

        repeat(count) { i ->
            segment.set(i, i)
        }

        repeat(count) { i ->
            assert(segment.get(i) == i)
        }
    }
}