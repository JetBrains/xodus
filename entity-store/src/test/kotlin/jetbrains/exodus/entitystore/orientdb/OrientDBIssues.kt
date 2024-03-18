package jetbrains.exodus.entitystore.orientdb

import com.orientechnologies.orient.core.db.ODatabaseSession
import com.orientechnologies.orient.core.metadata.schema.OClass
import com.orientechnologies.orient.core.record.OVertex

object Issues {
    const val CLASS = "Issue"
    const val NAME_PROPERTY = "name"
    const val PRIORITY_PROPERTY = "priority"

    object Links {
        const val IN_PROJECT = "InProject"
        const val ON_BOARD = "OnBoard"
    }
}

object Projects {
    const val CLASS = "Project"
    const val NAME_PROPERTY = "name"

    object Links {
        const val HAS_ISSUE = "HasIssue"
    }
}

object Boards {
    const val CLASS = "Board"
    const val NAME_PROPERTY = "name"

    object Links {
        const val HAS_ISSUE = "HasIssue"
    }
}

fun InMemoryOrientDB.createIssue(name: String, priority: String? = null): OVertexEntity {
    return withSession { session ->
        val issue = session.createNamedEntity(Issues.CLASS, name)
        priority?.let { issue.setProperty(Issues.PRIORITY_PROPERTY, it) }
        issue.save()
        issue
    }
}

fun InMemoryOrientDB.createProject(name: String): OVertexEntity {
    return withSession { session ->
        session.createNamedEntity(Projects.CLASS, name)
    }
}

fun InMemoryOrientDB.createBoard(name: String): OVertexEntity {
    return withSession { session ->
        session.createNamedEntity(Boards.CLASS, name)
    }
}

fun InMemoryOrientDB.addIssueToProject(
    issue: OEntity,
    project: OEntity
) {
    withSession { session ->
        session.getOrCreateEdgeClass(Issues.Links.IN_PROJECT)
        issue.addLink(Issues.Links.IN_PROJECT, project)

        session.getOrCreateEdgeClass(Projects.Links.HAS_ISSUE)
        project.addLink(Projects.Links.HAS_ISSUE, issue)
    }
}

fun InMemoryOrientDB.addIssueToBoard(
    issue: OEntity,
    board: OEntity
) {
    withSession { session ->
        session.getOrCreateEdgeClass(Issues.Links.ON_BOARD)
        issue.addLink(Issues.Links.ON_BOARD, board)

        session.getOrCreateEdgeClass(Projects.Links.HAS_ISSUE)
        board.addLink(Projects.Links.HAS_ISSUE, issue)
    }
}


private fun ODatabaseSession.createNamedEntity(
    className: String,
    name: String
): OVertexEntity {
    val projectClass = this.getOrCreateVertexClass(className)
    val project = this.newVertex(projectClass)
    project.setProperty(Projects.NAME_PROPERTY, name)
    project.save<OVertex>()
    return OVertexEntity(project)
}

private fun ODatabaseSession.getOrCreateVertexClass(className: String): OClass {
    return this.getClass(className) ?: this.createVertexClass(className)
}

private fun ODatabaseSession.getOrCreateEdgeClass(className: String): OClass {
    return this.getClass(className) ?: this.createEdgeClass(className)
}

