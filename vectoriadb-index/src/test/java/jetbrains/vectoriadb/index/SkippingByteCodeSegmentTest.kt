package jetbrains.vectoriadb.index

import org.junit.Test
import java.lang.foreign.Arena
import java.lang.foreign.ValueLayout

class SkippingByteCodeSegmentTest {

    @Test
    fun `simple set, get`() = Arena.ofConfined().use { arena ->
        val count = 256
        val mSegment = arena.allocate(count.toLong(), ValueLayout.JAVA_BYTE.byteAlignment())
        val segment = SkippingByteCodeSegment(mSegment, 1, 0)

        assert(segment.maxNumberOfCodes() == count)
        assert(segment.count() == count)

        repeat(count) { i ->
            segment.set(i, i)
        }

        repeat(count) { i ->
            assert(segment.get(i) == i)
        }
    }

    @Test
    fun `codes for a single vector are together in the memory`() = Arena.ofConfined().use { arena ->
        val codeCount = 3
        val vectorCount = 10
        val mSegment = arena.allocate(codeCount.toLong() * vectorCount, ValueLayout.JAVA_BYTE.byteAlignment())
        val segments = ByteCodeSegment.makeNativeSegments(mSegment, codeCount)

        repeat(codeCount) { codeIdx ->
            repeat(vectorCount) { vectorIdx ->
                segments[codeIdx].set(vectorIdx, 20 * codeIdx + vectorIdx)
            }
        }

        repeat(codeCount) { codeIdx ->
            repeat(vectorCount) { vectorIdx ->
                assert(segments[codeIdx].get(vectorIdx) == 20 * codeIdx + vectorIdx)
            }
        }

        repeat(vectorCount) { vectorIdx ->
            repeat(codeCount) { codeIdx ->
                val value = mSegment.getAtIndex(ValueLayout.JAVA_BYTE, codeCount.toLong() * vectorIdx + codeIdx).toUByte().toInt()
                assert(value == 20 * codeIdx + vectorIdx)
            }
        }
    }
}