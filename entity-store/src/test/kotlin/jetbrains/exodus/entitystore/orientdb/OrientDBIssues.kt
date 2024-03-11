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

// Many issues to One project
object ProjectIssues {
    const val ISSUE_T0_PROJECT = "issue_project"
    const val PROJECT_TO_ISSUES = "project_issues"
}

fun InMemoryOrientDB.createIssue(
    name: String,
    priority: String? = null
): OVertexEntity {
    return withSession { session ->
        val issueClass = session.getOrCreateVertexClass(IssueClass.NAME)
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
    return withSession { session ->
        val projectClass = session.getOrCreateVertexClass(ProjectClass.NAME)
        val project = session.newVertex(projectClass)
        project.setProperty(ProjectClass.NAME_PROPERTY, name)
        project.save<OVertex>()
        OVertexEntity(project)
    }
}


fun InMemoryOrientDB.linkIssueToProject(
    issue: OEntity,
    project: OEntity
) {
    withSession { session ->
        session.getOrCreateEdgeClass(ProjectIssues.ISSUE_T0_PROJECT)
        session.getOrCreateEdgeClass(ProjectIssues.PROJECT_TO_ISSUES)
        issue.addLink(ProjectIssues.ISSUE_T0_PROJECT, project)
        project.addLink(ProjectIssues.PROJECT_TO_ISSUES, issue)
    }
}

private fun ODatabaseSession.getOrCreateVertexClass(className: String): OClass {
    return this.getClass(className) ?: this.createVertexClass(className)
}

private fun ODatabaseSession.getOrCreateEdgeClass(className: String): OClass {
    return this.getClass(className) ?: this.createEdgeClass(className)
}

