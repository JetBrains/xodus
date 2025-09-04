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
package jetbrains.exodus.entitystore.youtrackdb.testutil

import com.google.common.truth.Ordered
import com.google.common.truth.Truth.assertThat
import com.jetbrains.youtrack.db.api.DatabaseSession
import jetbrains.exodus.entitystore.Entity
import jetbrains.exodus.entitystore.youtrackdb.YTDBEntity
import jetbrains.exodus.entitystore.youtrackdb.YTDBGremlinStoreTransactionImpl
import jetbrains.exodus.entitystore.youtrackdb.YTDBStoreTransaction
import jetbrains.exodus.entitystore.youtrackdb.YTDBVertexEntity

interface OTestMixin {

    val youTrackDb: InMemoryYouTrackDB

    fun assertNamesExactly(result: Iterable<Entity>, vararg names: String): Ordered {
        return assertThat(result.map { it.getProperty("name") }).containsExactly(*names)
    }

    fun assertNamesExactlyInOrder(result: Iterable<Entity>, vararg names: String) {
        assertNamesExactly(result, *names).inOrder()
    }

    fun beginTransaction(): YTDBGremlinStoreTransactionImpl {
        val store = youTrackDb.store
        return store.beginTransaction() as YTDBGremlinStoreTransactionImpl
    }

    fun <R> withStoreTx(block: (YTDBStoreTransaction) -> R): R {
        return youTrackDb.withStoreTx(block)
    }

    fun <R> withSession(block: (DatabaseSession) -> R): R {
        return youTrackDb.withSession(block)
    }

    fun <R> withTxSession(block: (DatabaseSession) -> R): R {
        return youTrackDb.withTxSession(block)
    }

    fun givenTestCase() = OTaskTrackerTestCase(youTrackDb)

    fun YTDBStoreTransaction.createIssue(name: String, priority: String? = null): YTDBVertexEntity = createIssueImpl(name, priority)

    fun YTDBStoreTransaction.createProject(name: String): YTDBVertexEntity = createProjectImpl(name)

    fun YTDBStoreTransaction.addIssueToProject(issue: YTDBEntity, project: YTDBEntity) =
        addIssueToProjectImpl(issue, project)

    fun YTDBStoreTransaction.addIssueToBoard(issue: YTDBEntity, board: YTDBEntity) =
        addIssueToBoardImpl(issue, board)
}