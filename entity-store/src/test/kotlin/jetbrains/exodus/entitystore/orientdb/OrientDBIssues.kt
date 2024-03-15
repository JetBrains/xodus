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
    }
}

object Projects {
    const val CLASS = "Project"
    const val NAME_PROPERTY = "name"

    object Links {
        const val HAS_ISSUE = "HasIssue"
    }
}

fun InMemoryOrientDB.createIssue(
    name: String,
    priority: String? = null
): OVertexEntity {
    return withSession { session ->
        val issueClass = session.getOrCreateVertexClass(Issues.CLASS)
        val issue = session.newVertex(issueClass)
        issue.setProperty(Issues.NAME_PROPERTY, name)
        priority?.let { issue.setProperty(Issues.PRIORITY_PROPERTY, it) }
        issue.save<OVertex>()
        OVertexEntity(issue)
    }
}


fun InMemoryOrientDB.createProject(
    name: String
): OVertexEntity {
    return withSession { session ->
        val projectClass = session.getOrCreateVertexClass(Projects.CLASS)
        val project = session.newVertex(projectClass)
        project.setProperty(Projects.NAME_PROPERTY, name)
        project.save<OVertex>()
        OVertexEntity(project)
    }
}


fun InMemoryOrientDB.linkIssueToProject(
    issue: OEntity,
    project: OEntity
) {
    withSession { session ->
        session.getOrCreateEdgeClass(Issues.Links.IN_PROJECT)
        session.getOrCreateEdgeClass(Projects.Links.HAS_ISSUE)
        issue.addLink(Issues.Links.IN_PROJECT, project)
        project.addLink(Projects.Links.HAS_ISSUE, issue)
    }
}

private fun ODatabaseSession.getOrCreateVertexClass(className: String): OClass {
    return this.getClass(className) ?: this.createVertexClass(className)
}

private fun ODatabaseSession.getOrCreateEdgeClass(className: String): OClass {
    return this.getClass(className) ?: this.createEdgeClass(className)
}

