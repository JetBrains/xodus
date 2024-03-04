package jetbrains.exodus.entitystore.orientdb

import com.orientechnologies.orient.core.record.OVertex

object IssueClass {
    const val NAME = "Issue"
    const val NAME_PROPERTY = "name"
}

fun InMemoryOrientDB.createIssue(name: String): OVertex {
    return withSession { session ->
        if (session.getClass(IssueClass.NAME) == null) {
            session.createVertexClass(IssueClass.NAME)
        }
        val issueClass = session.getClass(IssueClass.NAME)
        val issue = session.newVertex(issueClass)
        issue.setProperty(IssueClass.NAME_PROPERTY, name)
        issue.save<OVertex>()
    }
}