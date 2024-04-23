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
package jetbrains.exodus.entitystore.orientdb.query

import jetbrains.exodus.entitystore.EntityIterable
import jetbrains.exodus.entitystore.orientdb.testutil.*
import org.junit.Rule
import org.junit.Test

class OQueryMaxTest {

    @Rule
    @JvmField
    val orientDB = InMemoryOrientDB()



    @Test
    fun `should query with links recursively`() {
        // Given
        val test = givenTestCase()
        orientDB.openSession()
        val additionalIssues = (0..200).map {
            orientDB.createIssue("trash-$it", "minor").also { issue->
                orientDB.addIssueToProject(issue, test.project3)
            }
        }
        (0..200).map {
            orientDB.createBoard("trash-board-$it").also { board->
                additionalIssues.forEach { issue->
                    orientDB.addIssueToBoard(issue, board)
                }
            }
        }

        // Issues assigned to projects
        orientDB.addIssueToProject(test.issue1, test.project1)
        orientDB.addIssueToProject(test.issue2, test.project1)
        orientDB.withSession {
            test.issue1.setProperty(Issues.Props.PRIORITY, "critical")
            test.issue2.setProperty(Issues.Props.PRIORITY, "critical")
        }
        orientDB.addIssueToBoard(test.issue2, test.board1)
        orientDB.addIssueToBoard(test.issue1, test.board1)

        //

        // When
        orientDB.store.executeInTransaction { tx ->
            val issuesInProject = tx.findLinks(Issues.CLASS, test.project1, Issues.Links.IN_PROJECT)

            fun query(issues: EntityIterable, level: Int = 0): EntityIterable {
                if (level == 8) {
                    return issues
                }
                val boards = tx.findLinks(Boards.CLASS, issues, Boards.Links.HAS_ISSUE).distinct()
                val issuesOnBoards = tx.findLinks(Issues.CLASS, boards, Issues.Links.ON_BOARD).distinct()
                val project = tx.sort(Projects.CLASS, "name", tx.findLinks(Projects.CLASS, issues, Projects.Links.HAS_ISSUE).distinct(),level % 2 == 1).distinct()
                var issues = tx.sort(Issues.CLASS, Issues.Props.PRIORITY, tx.findLinks(Issues.CLASS, project, Issues.Links.IN_PROJECT), level % 2 == 0).distinct()
                issues = issues.intersect(issuesOnBoards)

                return query(issues, level + 1)
            }

            val time = System.currentTimeMillis()
            val issues = query(issuesInProject).toList()
            println(issues.size)
            println(issues.map { it.getProperty("name") })
            println("Spent time on calculation ${System.currentTimeMillis() - time}ms")
        }
    }

    private fun givenTestCase() = OTaskTrackerTestCase(orientDB)
}
