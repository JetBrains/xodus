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