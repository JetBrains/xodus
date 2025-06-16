package jetbrains.exodus.entitystore.youtrackdb.gremlin

import com.jetbrains.youtrack.db.api.gremlin.YTDBVertex
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal

class GremlinQueryPropertyEqual(private val property: String, private val value: Object) : GremlinQuery {
    override fun traverse(g: GraphTraversal<*, YTDBVertex>): GraphTraversal<*, YTDBVertex> =
        g.has(property, value)
}