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

import com.jetbrains.youtrack.db.api.exception.ModificationOperationProhibitedException
import jetbrains.exodus.entitystore.youtrackdb.testutil.InMemoryYouTrackDB
import jetbrains.exodus.entitystore.youtrackdb.testutil.OTestMixin
import org.junit.Rule
import org.junit.Test
import kotlin.test.assertFailsWith

class YTDBDatabaseProviderTest: OTestMixin {

    @Rule
    @JvmField
    val youtrackDbRule = InMemoryYouTrackDB()

    override val youTrackDb = youtrackDbRule

    @Test
    fun `read-only mode works`() {
        // by default it is read-write
        withTxSession { session ->
            session.activeTransaction.newVertex()
        }

        youTrackDb.provider.readOnly = true
        assertFailsWith<ModificationOperationProhibitedException> {
            withTxSession { session ->
                session.activeTransaction.newVertex()
            }
        }

        youTrackDb.provider.readOnly = false
        withTxSession { session ->
            session.activeTransaction.newVertex()
        }

        youTrackDb.provider.readOnly = true
        // close() releases the read-only mode before closing the database (otherwise it throws exceptions)
        youTrackDb.provider.close()
    }
}