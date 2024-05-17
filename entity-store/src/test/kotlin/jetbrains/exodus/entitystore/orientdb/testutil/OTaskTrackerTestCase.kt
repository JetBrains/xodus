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

class OTaskTrackerTestCase(val orientDB: InMemoryOrientDB) {

    val project1 = orientDB.createProject("project1")
    val project2 = orientDB.createProject("project2")
    val project3 = orientDB.createProject("project3")

    val issue1 = orientDB.createIssue("issue1")
    val issue2 = orientDB.createIssue("issue2")
    val issue3 = orientDB.createIssue("issue3")

    val board1 = orientDB.createBoard("board1")
    val board2 = orientDB.createBoard("board2")
    val board3 = orientDB.createBoard("board3")


    fun createManyIssues(count: Int) {
        for (i in 1..count) {
            orientDB.createIssue("issue$i")
        }
    }
}