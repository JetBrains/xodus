package jetbrains.exodus.entitystore.orientdb

import com.orientechnologies.orient.core.db.ODatabaseSession
import com.orientechnologies.orient.core.metadata.schema.OClass
import com.orientechnologies.orient.core.record.OVertex

object IssueClass {
    const val NAME = "Issue"
    const val NAME_PROPERTY = "name"
    const val PRIORITY_PROPERTY = "priority"
}

object ProjectClass {
    const val NAME = "Project"
    const val NAME_PROPERTY = "name"
}

fun InMemoryOrientDB.createIssue(
    name: String,
    priority: String? = null
): OVertexEntity {
    return withTxSession { session ->
        val issueClass = session.getOrCreateClass(IssueClass.NAME)
        val issue = session.newVertex(issueClass)
        issue.setProperty(IssueClass.NAME_PROPERTY, name)
        priority?.let { issue.setProperty(IssueClass.PRIORITY_PROPERTY, it) }
        issue.save<OVertex>()
        OVertexEntity(issue)
    }
}


fun InMemoryOrientDB.createProject(
    name: String
): OVertexEntity {
    return withTxSession { session ->
        val projectClass = session.getOrCreateClass(ProjectClass.NAME)
        val project = session.newVertex(projectClass)
        project.setProperty(ProjectClass.NAME_PROPERTY, name)
        project.save<OVertex>()
        OVertexEntity(project)
    }
}


fun InMemoryOrientDB.createIssueLink(
    from: OVertex,
    to: OVertex
) {
    withTxSession { session ->
        val link = session.newEdge(from, to, "Link")
        link.save<OVertex>()
    }
}

private fun ODatabaseSession.getOrCreateClass(className: String): OClass {
    return this.getClass(className) ?: this.createVertexClass(className)
}

