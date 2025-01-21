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
package jetbrains.exodus.entitystore.youtrackdb

import jetbrains.exodus.entitystore.youtrackdb.testutil.InMemoryYouTrackDB
import jetbrains.exodus.entitystore.youtrackdb.testutil.Issues
import jetbrains.exodus.entitystore.youtrackdb.testutil.createIssueImpl
import org.junit.Assert
import org.junit.Rule
import org.junit.Test

class DBCompactTest {

    @Rule
    @JvmField
    val orientDb = InMemoryYouTrackDB()

    @Test
    fun `database compacter should work`() {
        orientDb.withStoreTx { tx ->
            (0..99).forEach {
                tx.createIssueImpl("Test issue $it")
            }
        }
        orientDb.provider.compact()
        orientDb.withStoreTx {
            val size = it.getAll(Issues.CLASS).size()
            Assert.assertEquals(100, size)
        }
    }
}
