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

import com.orientechnologies.orient.core.db.ODatabaseSession
import com.orientechnologies.orient.core.metadata.schema.OType
import com.orientechnologies.orient.core.record.ODirection
import com.orientechnologies.orient.core.record.OVertex
import jetbrains.exodus.entitystore.Entity
import jetbrains.exodus.entitystore.orientdb.OEntity
import jetbrains.exodus.entitystore.orientdb.OStoreTransaction
import jetbrains.exodus.entitystore.orientdb.OVertexEntity
import jetbrains.exodus.entitystore.orientdb.testutil.Issues.CLASS
import jetbrains.exodus.entitystore.orientdb.testutil.Issues.Links.IN_PROJECT
import jetbrains.exodus.entitystore.orientdb.testutil.Issues.Links.ON_BOARD
import jetbrains.exodus.entitystore.orientdb.testutil.Issues.Props.PRIORITY
import jetbrains.exodus.entitystore.orientdb.testutil.Projects.Links.HAS_ISSUE

object Issues {
    const val CLASS = "Issue"

    object Props {
        const val PRIORITY = "priority"
    }

    object Links {
        const val IN_PROJECT = "InProject"
        const val ON_BOARD = "OnBoard"
    }
}

object Projects {
    const val CLASS = "Project"

    object Links {
        const val HAS_ISSUE = "HasIssue"
    }
}

object Boards {
    const val CLASS = "Board"

    object Links {
        const val HAS_ISSUE = "HasIssue"
    }
}

fun InMemoryOrientDB.createIssue(name: String, priority: String? = null): OVertexEntity {
    return withStoreTx { tx ->
        tx.createIssueImpl(name, priority)
    }
}

fun InMemoryOrientDB.addIssueToProject(issue: OEntity, project: OEntity) {
    withStoreTx { tx ->
        tx.addIssueToProjectImpl(issue, project)
    }
}

fun InMemoryOrientDB.addIssueToBoard(issue: OEntity, board: OEntity) {
    withSession { session ->
        session.addAssociation(Issues.CLASS, Boards.CLASS, ON_BOARD, HAS_ISSUE)
        session.addAssociation(Boards.CLASS, Issues.CLASS, HAS_ISSUE, ON_BOARD)
    }
    withStoreTx {
        issue.addLink(ON_BOARD, board)
        board.addLink(Boards.Links.HAS_ISSUE, issue)
    }
}

fun OEntity.name(): Comparable<*> {
    return getProperty("name") ?: throw IllegalStateException("Entity has no name property")
}

internal fun OStoreTransaction.createProjectImpl(name: String): OVertexEntity {
    val e = newEntity(Projects.CLASS) as OVertexEntity
    e.setName(name)
    return e
}

private fun Entity.setName(name: String) {
    setProperty("name", name)
}

internal fun OStoreTransaction.createIssueImpl(name: String, priority: String? = null): OVertexEntity {
    val issue = newEntity(CLASS) as OVertexEntity
    issue.setName(name)
    priority?.let { issue.setProperty(PRIORITY, it) }
    return issue
}

internal fun OStoreTransaction.createBoardImpl(name: String): OVertexEntity {
    val e = newEntity(Boards.CLASS) as OVertexEntity
    e.setName(name)
    return e
}

internal fun OStoreTransaction.addIssueToProjectImpl(issue: OEntity, project: OEntity) {
    issue.addLink(IN_PROJECT, project)
    project.addLink(HAS_ISSUE, issue)
}

internal fun OStoreTransaction.addIssueToBoardImpl(issue: OEntity, board: OEntity) {
    issue.addLink(ON_BOARD, board)
    board.addLink(Boards.Links.HAS_ISSUE, issue)
}

internal fun ODatabaseSession.addAssociation(
    fromClassName: String,
    toClassName: String,
    outPropName: String,
    inPropName: String
) {
    val fromClass = getClass(fromClassName) ?: throw IllegalStateException("$fromClassName not found")
    val toClass = getClass(toClassName) ?: throw IllegalStateException("$toClassName not found")
    val inEdgeName = OVertexEntity.edgeClassName(inPropName)
    val outEdgeName = OVertexEntity.edgeClassName(outPropName)
    getClass(inEdgeName) ?: this.createEdgeClass(inEdgeName)
    getClass(outEdgeName) ?: this.createEdgeClass(outEdgeName)

    val linkInPropName = OVertex.getEdgeLinkFieldName(ODirection.IN, inEdgeName)
    val linkOutPropName = OVertex.getEdgeLinkFieldName(ODirection.OUT, outEdgeName)
    fromClass.createProperty(linkOutPropName, OType.LINKBAG)
    toClass.createProperty(linkInPropName, OType.LINKBAG)
}