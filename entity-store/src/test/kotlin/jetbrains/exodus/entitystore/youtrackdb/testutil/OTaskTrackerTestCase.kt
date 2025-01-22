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

import jetbrains.exodus.entitystore.youtrackdb.YTDBStoreTransaction
import jetbrains.exodus.entitystore.youtrackdb.YTDBVertexEntity

class OTaskTrackerTestCase(val orientDB: InMemoryYouTrackDB) {

    val project1: YTDBVertexEntity
    val project2: YTDBVertexEntity
    val project3: YTDBVertexEntity

    val issue1: YTDBVertexEntity = orientDB.createIssue("issue1")
    val issue2: YTDBVertexEntity = orientDB.createIssue("issue2")
    val issue3: YTDBVertexEntity = orientDB.createIssue("issue3")

    val board1: YTDBVertexEntity
    val board2: YTDBVertexEntity
    val board3: YTDBVertexEntity

    init {
        val tx = orientDB.store.beginTransaction() as YTDBStoreTransaction
        project1 = tx.createProjectImpl("project1")
        project2 = tx.createProjectImpl("project2")
        project3 = tx.createProjectImpl("project3")
        board1 = tx.createBoardImpl("board1")
        board2 = tx.createBoardImpl("board2")
        board3 = tx.createBoardImpl("board3")
        tx.commit()
    }

    fun createManyIssues(count: Int) {
        for (i in 1..count) {
            orientDB.createIssue("issue$i")
        }
    }
}