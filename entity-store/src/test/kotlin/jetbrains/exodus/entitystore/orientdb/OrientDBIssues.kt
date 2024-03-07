package jetbrains.exodus.entitystore.orientdb

import com.orientechnologies.orient.core.record.OVertex

object IssueClass {
    const val NAME = "Issue"
    const val NAME_PROPERTY = "name"
    const val PROJECT_PROPERTY = "project"
}

fun InMemoryOrientDB.createIssue(
    name: String,
    project: String? = null
): OVertex {
    return withSession { session ->
        if (session.getClass(IssueClass.NAME) == null) {
            session.createVertexClass(IssueClass.NAME)
        }
        val issueClass = session.getClass(IssueClass.NAME)

        val issue = session.newVertex(issueClass)

        issue.setProperty(IssueClass.NAME_PROPERTY, name)
        project?.let { issue.setProperty(IssueClass.PROJECT_PROPERTY, it) }

        issue.save<OVertex>()
    }
}