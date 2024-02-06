/*
 * Copyright ${inceptionYear} - ${year} ${owner}
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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