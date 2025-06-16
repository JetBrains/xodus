package jetbrains.exodus.entitystore.youtrackdb.gremlin

import com.jetbrains.youtrack.db.api.gremlin.YTDBVertex
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal

interface GremlinQuery {

    fun traverse(g: GraphTraversal<*, YTDBVertex>): GraphTraversal<*, YTDBVertex>

    // TODO: not stack safe potentially?
    fun andThen(query: GremlinQuery) = object : GremlinQuery {
        override fun traverse(g: GraphTraversal<*, YTDBVertex>): GraphTraversal<*, YTDBVertex> {
            val h = this.traverse(g)
            val i = query.traverse(h)
            return i
        }
    }

//    fun execute(session: DatabaseSession): List<Map<String, Any>> {
//        val a = session.asGraph()
//            .traversal().V()
//            .hasLabel("Abc")
//            .has("name", "xyz")
//            .out()
//            .out()
//            .has("name", "abc")
//            .filter(`__`.out("aaa").count().`is`(gt(2)))
//            .filter(`__`.out("aaa").count().`is`(gt(2)))
//
//        return emptyList()
//    }
}