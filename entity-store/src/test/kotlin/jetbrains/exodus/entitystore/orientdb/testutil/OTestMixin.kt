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
package jetbrains.exodus.entitystore.orientdb.testutil

import com.google.common.truth.Ordered
import com.google.common.truth.Truth.assertThat
import com.orientechnologies.orient.core.db.ODatabaseSession
import jetbrains.exodus.entitystore.Entity
import jetbrains.exodus.entitystore.orientdb.OEntity
import jetbrains.exodus.entitystore.orientdb.OStoreTransaction
import jetbrains.exodus.entitystore.orientdb.OStoreTransactionImpl
import jetbrains.exodus.entitystore.orientdb.OVertexEntity

interface OTestMixin {

    val orientDb: InMemoryOrientDB

    fun assertNamesExactly(result: Iterable<Entity>, vararg names: String): Ordered {
        return assertThat(result.map { it.getProperty("name") }).containsExactly(*names)
    }

    fun assertNamesExactlyInOrder(result: Iterable<Entity>, vararg names: String) {
        assertNamesExactly(result, *names).inOrder()
    }

    fun beginTransaction(): OStoreTransactionImpl {
        val store = orientDb.store
        return store.beginTransaction() as OStoreTransactionImpl
    }

    fun beginReadonlyTransaction(): OStoreTransactionImpl {
        val store = orientDb.store
        return store.beginReadonlyTransaction() as OStoreTransactionImpl
    }

    fun <R> withStoreTx(block: (OStoreTransaction) -> R): R {
        return orientDb.withStoreTx(block)
    }

    fun <R> withSession(block: (ODatabaseSession) -> R): R {
        return orientDb.withSession(block)
    }

    fun givenTestCase() = OTaskTrackerTestCase(orientDb)

    fun OStoreTransaction.createIssue(name: String, priority: String? = null): OVertexEntity = createIssueImpl(name, priority)

    fun OStoreTransaction.createProject(name: String): OVertexEntity = createProjectImpl(name)

    fun OStoreTransaction.createBoard(name: String): OVertexEntity = createBoardImpl(name)

    fun OStoreTransaction.addIssueToProject(issue: OEntity, project: OEntity) = addIssueToProjectImpl(issue, project)

    fun OStoreTransaction.addIssueToBoard(issue: OEntity, board: OEntity) = addIssueToBoardImpl(issue, board)
}