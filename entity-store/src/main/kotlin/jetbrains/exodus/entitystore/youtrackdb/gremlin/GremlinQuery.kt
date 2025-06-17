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

    class Dedup : GremlinQuery {
        override fun traverse(g: GraphTraversal<*, YTDBVertex>): GraphTraversal<*, YTDBVertex> = g.dedup()
    }

    class Label(val entityType: String) : GremlinQuery {
        override fun traverse(g: GraphTraversal<*, YTDBVertex>): GraphTraversal<*, YTDBVertex> =
            g.hasLabel(entityType)
    }

    class Limit(val limit: Long) : GremlinQuery {
        override fun traverse(g: GraphTraversal<*, YTDBVertex>): GraphTraversal<*, YTDBVertex> =
            g.limit(limit)
    }

    class Skip(val skip: Long) : GremlinQuery {
        override fun traverse(g: GraphTraversal<*, YTDBVertex>): GraphTraversal<*, YTDBVertex> =
            g.skip(skip)
    }

    class Tail() : GremlinQuery {
        override fun traverse(g: GraphTraversal<*, YTDBVertex>): GraphTraversal<*, YTDBVertex> =
            g.tail()
    }

    class PropEqual(private val property: String, private val value: Object) : GremlinQuery {
        override fun traverse(g: GraphTraversal<*, YTDBVertex>): GraphTraversal<*, YTDBVertex> =
            g.has(property, value)
    }

    class Reverse : GremlinQuery {
        override fun traverse(g: GraphTraversal<*, YTDBVertex>): GraphTraversal<*, YTDBVertex> =
            g.reverse()
    }
}

