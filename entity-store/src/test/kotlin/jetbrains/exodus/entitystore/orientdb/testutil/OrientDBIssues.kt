package jetbrains.exodus.entitystore.orientdb.testutil

import com.orientechnologies.orient.core.db.ODatabaseSession
import com.orientechnologies.orient.core.metadata.schema.OClass
import com.orientechnologies.orient.core.record.OVertex
import jetbrains.exodus.entitystore.PersistentEntityStore
import jetbrains.exodus.entitystore.orientdb.OEntity
import jetbrains.exodus.entitystore.orientdb.OVertexEntity
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
    return withSession { session ->
        val issue = session.createNamedEntity(CLASS, name, store)
        priority?.let { issue.setProperty(PRIORITY, it) }
        issue.save()
        issue
    }
}

fun InMemoryOrientDB.createProject(name: String): OVertexEntity {
    return withSession { session ->
        session.createNamedEntity(Projects.CLASS, name, store)
    }
}

fun InMemoryOrientDB.createBoard(name: String): OVertexEntity {
    return withSession { session ->
        session.createNamedEntity(Boards.CLASS, name, store)
    }
}

fun InMemoryOrientDB.addIssueToProject(issue: OEntity, project: OEntity) {
    withSession { session ->
        session.getOrCreateEdgeClass(IN_PROJECT)
        issue.addLink(IN_PROJECT, project)

        session.getOrCreateEdgeClass(HAS_ISSUE)
        project.addLink(HAS_ISSUE, issue)
    }
}

fun InMemoryOrientDB.addIssueToBoard(issue: OEntity, board: OEntity) {
    withSession { session ->
        session.getOrCreateEdgeClass(ON_BOARD)
        issue.addLink(ON_BOARD, board)

        session.getOrCreateEdgeClass(HAS_ISSUE)
        board.addLink(Boards.Links.HAS_ISSUE, issue)
    }
}

private fun ODatabaseSession.createNamedEntity(
    className: String,
    name: String,
    store: PersistentEntityStore
): OVertexEntity {
    val oClass = this.getOrCreateVertexClass(className)
    val entity = this.newVertex(oClass)
    entity.setProperty("name", name)
    entity.save<OVertex>()
    return OVertexEntity(entity, store)
}

private fun ODatabaseSession.getOrCreateVertexClass(className: String): OClass {
    return this.getClass(className) ?: this.createVertexClass(className)
}

private fun ODatabaseSession.getOrCreateEdgeClass(className: String): OClass {
    return this.getClass(className) ?: this.createEdgeClass(className)
}

fun OEntity.name(): Comparable<*> {
    return getProperty("name") ?: throw IllegalStateException("Entity has no name property")
}