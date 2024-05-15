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
import com.orientechnologies.orient.core.metadata.schema.OClass
import com.orientechnologies.orient.core.record.OVertex
import jetbrains.exodus.entitystore.PersistentEntityStore
import jetbrains.exodus.entitystore.orientdb.*
import jetbrains.exodus.entitystore.orientdb.testutil.Issues.CLASS
import jetbrains.exodus.entitystore.orientdb.testutil.Issues.Links.IN_PROJECT
import jetbrains.exodus.entitystore.orientdb.testutil.Issues.Links.ON_BOARD
import jetbrains.exodus.entitystore.orientdb.testutil.Issues.Props.PRIORITY
import jetbrains.exodus.entitystore.orientdb.testutil.Projects.Links.HAS_ISSUE
import kotlin.let

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
    provider.acquireSession().use { session ->
        session.getOrCreateVertexClass(CLASS)
    }
    return withSession { session ->
        val issue = session.createNamedEntity(CLASS, name, store)
        priority?.let { issue.setProperty(PRIORITY, it) }
        issue.save()
        issue
    }
}

fun InMemoryOrientDB.createProject(name: String): OVertexEntity {
    provider.acquireSession().use { session ->
        session.getOrCreateVertexClass(Projects.CLASS)
    }
    return withSession { session ->
        session.createNamedEntity(Projects.CLASS, name, store)
    }
}

fun InMemoryOrientDB.createBoard(name: String): OVertexEntity {
    provider.acquireSession().use { session ->
        session.getOrCreateVertexClass(Boards.CLASS)
    }
    return withSession { session ->
        session.createNamedEntity(Boards.CLASS, name, store)
    }
}

fun InMemoryOrientDB.addIssueToProject(issue: OEntity, project: OEntity) {
    provider.acquireSession().use { session ->
        session.getOrCreateEdgeClass(IN_PROJECT)
        session.getOrCreateEdgeClass(HAS_ISSUE)
    }

    withSession {
        issue.addLink(IN_PROJECT, project)
        project.addLink(HAS_ISSUE, issue)
    }
}

fun InMemoryOrientDB.addIssueToBoard(issue: OEntity, board: OEntity) {
    provider.acquireSession().use { session ->
        session.getOrCreateEdgeClass(ON_BOARD)
        session.getOrCreateEdgeClass(HAS_ISSUE)
    }
    withSession {
        issue.addLink(ON_BOARD, board)
        board.addLink(Boards.Links.HAS_ISSUE, issue)
    }
}

fun ODatabaseSession.createNamedEntity(
    className: String,
    name: String,
    store: PersistentEntityStore
): OVertexEntity {
    val oClass = this.getClass(className) ?: throw IllegalStateException("Create class $className before using it")
    val entity = this.newVertex(oClass)
    setLocalEntityIdIfAbsent(entity)
    entity.setProperty("name", name)
    entity.save<OVertex>()
    return OVertexEntity(entity, store)
}

private fun ODatabaseSession.getOrCreateEdgeClass(className: String): OClass {
    return this.getClass(className) ?: this.createEdgeClass(className)
}

fun OEntity.name(): Comparable<*> {
    return getProperty("name") ?: throw IllegalStateException("Entity has no name property")
}
