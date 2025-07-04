package jetbrains.exodus.entitystore.youtrackdb.gremlin

import com.jetbrains.youtrack.db.api.gremlin.YTDBVertex
import com.jetbrains.youtrack.db.api.record.RID
import jetbrains.exodus.entitystore.youtrackdb.YTDBVertexEntity
import org.apache.commons.lang3.StringUtils
import org.apache.tinkerpop.gremlin.process.traversal.Order
import org.apache.tinkerpop.gremlin.process.traversal.P
import org.apache.tinkerpop.gremlin.process.traversal.TextP
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__

typealias YT = GraphTraversal<*, YTDBVertex>

abstract class GremlinQuery(val shortName: String) {

    abstract fun traverse(g: YT): YT

    abstract fun describe(s: StringBuilder): StringBuilder

    abstract fun describeGremlin(s: StringBuilder): StringBuilder

    open fun simplify(): GremlinQuery? = null

    abstract class GremlinUnaryOp(shortName: String) : GremlinQuery(shortName)
    abstract class GremlinBinaryOp(shortName: String) : GremlinQuery(shortName)

    // TODO: not stack safe potentially?
    fun andThen(query: GremlinQuery) =
        if (this is All) query
        else if (query is All) this
        else AndThen(this, query)

    data class AndThen(val left: GremlinQuery, val right: GremlinQuery) : GremlinBinaryOp("andThen") {
        override fun traverse(g: YT): YT {
            val h = left.traverse(g)
            val i = right.traverse(h)
            return i
        }

        override fun describeGremlin(s: StringBuilder): StringBuilder =
            right.describeGremlin(
                left.describeGremlin(s).appendLine()
            )

        override fun describe(s: StringBuilder) = right.describe(left.describe(s).append(", THEN "))
        override fun simplify(): GremlinQuery? =
            when {
                left is All -> right
                right is All -> left
                else -> null
            }
    }

    data class Or(val left: GremlinQuery, val right: GremlinQuery) : GremlinBinaryOp("or") {
        override fun traverse(g: YT): YT =
            g.or(
                left.traverse(`__`.start<Any>().asYT()),
                right.traverse(`__`.start<Any>().asYT())
            )

        override fun describeGremlin(s: StringBuilder): StringBuilder {
            s.appendLine(".or(")
            left.describeGremlin(s).appendLine()
            right.describeGremlin(s).appendLine()
            s.appendLine(")")
            return s
        }

        override fun describe(s: StringBuilder) = right.describe(left.describe(s).append(" OR "))
        override fun simplify(): GremlinQuery? =
            if (left is All || right is All) All
            else null
    }

    data class And(val left: GremlinQuery, val right: GremlinQuery) : GremlinBinaryOp("and") {
        override fun traverse(g: YT): YT =
            g.and(
                left.traverse(`__`.start<Any>().asYT()),
                right.traverse(`__`.start<Any>().asYT())
            )

        override fun describeGremlin(s: StringBuilder): StringBuilder {
            s.appendLine(".and(")
            left.describeGremlin(s).appendLine()
            right.describeGremlin(s).appendLine()
            s.appendLine(")")
            return s
        }

        override fun describe(s: StringBuilder): StringBuilder = right.describe(left.describe(s).append(" AND "))
        override fun simplify(): GremlinQuery? =
            when {
                left is All -> right
                right is All -> left
                else -> null
            }
    }

    data class Not(val query: GremlinQuery) : GremlinUnaryOp("not") {
        override fun traverse(g: YT): YT = g.not(query.traverse(`__`.start<Any>().asYT()))
        override fun describeGremlin(s: StringBuilder): StringBuilder {
            s.appendLine(".not(")
            query.describeGremlin(s).appendLine()
            s.appendLine(")")
            return s
        }

        override fun describe(s: StringBuilder): StringBuilder = query.describe(s.append("NOT "))
        override fun simplify(): GremlinQuery? =
            when (query) {
                is Not -> query.query
                else -> null
            }
    }

    data object All : GremlinQuery("all") {
        override fun traverse(g: YT): YT = g
        override fun describe(s: StringBuilder): java.lang.StringBuilder = s.append("*")
        override fun describeGremlin(s: StringBuilder): StringBuilder {
            return s
        }
    }

    data object Dedup : GremlinQuery("dedup") {
        override fun traverse(g: YT): YT = g.dedup()
        override fun describe(s: StringBuilder): StringBuilder = s.append(".dedup()")
        override fun describeGremlin(s: StringBuilder): StringBuilder {
            return s.append(".dedup()")
        }
    }

    data class HasLabel(val entityType: String) : GremlinQuery("hl") {
        override fun traverse(g: YT): YT = g.hasLabel(entityType)
        override fun describe(s: StringBuilder): StringBuilder = s.append(".hasLabel(").append(entityType).append(")")
        override fun describeGremlin(s: StringBuilder): StringBuilder {
            return s.append(".hasLabel(").append(entityType).append(")")
        }
    }

    data class Limit(val limit: Long) : GremlinQuery("lim") {
        override fun traverse(g: YT): YT = g.limit(limit)
        override fun describe(s: StringBuilder): StringBuilder = s.append(".limit(").append(limit).append(")")
        override fun describeGremlin(s: StringBuilder): StringBuilder {
            return s.append(".limit(").append(limit).append(")")
        }
    }

    data class Skip(val skip: Long) : GremlinQuery("skp") {
        override fun traverse(g: YT): YT = g.skip(skip)
        override fun describe(s: StringBuilder): StringBuilder = s.append(".skip(").append(skip).append(")")
        override fun describeGremlin(s: StringBuilder): StringBuilder {
            return s.append(".skip(").append(skip).append(")")
        }
    }

    data object Tail : GremlinQuery("tail") {
        override fun traverse(g: YT): YT = g.tail()
        override fun describe(s: StringBuilder): StringBuilder = s.append(".tail()")
        override fun describeGremlin(s: StringBuilder): StringBuilder {
            return s.append(".tail()")
        }
    }

    data class PropEqual(val property: String, val value: Any?) : GremlinQuery("eq") {
        override fun traverse(g: YT): YT = g.has(property, value)
        override fun describe(s: StringBuilder): StringBuilder = s.append(property).append("=").append(value)
        override fun describeGremlin(s: StringBuilder): StringBuilder {
            return s.append("has(").append(property).append(", ").append(value).append(")")
        }
    }

    data class PropNull(val property: String) : GremlinQuery("nul") {
        override fun traverse(g: YT): YT = g.hasNot(property)
        override fun describe(s: StringBuilder): StringBuilder = s.append(property).append(" IS NULL")
        override fun describeGremlin(s: StringBuilder): StringBuilder {
            return s.append("hasNot(").append(property).append(")")
        }
    }

    data class PropNotNull(val property: String) : GremlinQuery("nn") {
        override fun traverse(g: YT): YT = g.has(property)
        override fun describe(s: StringBuilder): StringBuilder = s.append(property).append(" IS NOT NULL")
        override fun describeGremlin(s: StringBuilder): StringBuilder {
            return s.append("has(").append(property).append(")")
        }
    }

    data object Reverse : GremlinQuery("rev") {
        override fun traverse(g: YT): YT = g.reverse()
        override fun describe(s: StringBuilder): StringBuilder = s.append(".reverse()")
        override fun describeGremlin(s: StringBuilder): StringBuilder {
            return s.append(".reverse()")
        }
    }

    data class Link(val linkName: String) : GremlinQuery("lnk") {
        override fun traverse(g: YT): YT = g.out(YTDBVertexEntity.edgeClassName(linkName)).asYT()
        override fun describe(s: StringBuilder): StringBuilder = s.append(".out(").append(linkName).append(")")
        override fun describeGremlin(s: StringBuilder): StringBuilder {
            return s.append(".out(").append(linkName).append(")")
        }
    }

    data class HasSubstring(val property: String, val substring: String?, val caseSensitive: Boolean) :
        GremlinQuery("hsub") {
        override fun traverse(g: YT): YT =
            if (caseSensitive) g.has(property, TextP.containing(substring))
            else g.where(
                `__`
                    .values<YTDBVertex, String>(property).toLower()
                    .`is`(TextP.containing(substring?.lowercase()))
            )

        override fun describeGremlin(s: StringBuilder): StringBuilder {
            return if (caseSensitive) s.append("has(").append(property).append(", containing(").append(substring)
                .append("))")
            else s.appendLine("where(")
                .append("values(").append(property).append(").toLower()")
                .append("is(containing(").append(substring?.lowercase()).append("))")

        }

        override fun describe(s: StringBuilder): StringBuilder =
            s.append(property).append(" hasSubstring ").append(substring)

        override fun simplify(): GremlinQuery? = if (StringUtils.isEmpty(substring)) All else null
    }

    data class HasPrefix(val property: String, val prefix: String, val caseSensitive: Boolean) : GremlinQuery("hp") {
        override fun traverse(g: YT): YT =
            if (caseSensitive) g.has(property, TextP.startingWith(prefix))
            else g.where(
                `__`
                    .values<YTDBVertex, String>(property).toLower()
                    .`is`(TextP.startingWith(prefix.lowercase()))
            )

        override fun describe(s: StringBuilder): StringBuilder =
            s.append(property).append(" hasPrefix ").append(prefix)

        override fun describeGremlin(s: StringBuilder): StringBuilder {
            return if (caseSensitive) s.append("has(").append(property).append(", startingWith(").append(prefix)
                .append("))")
            else s.appendLine("where(")
                .append("values(").append(property).append(").toLower()")
                .append("is(startingWith(").append(prefix.lowercase()).append("))")
        }

        override fun simplify(): GremlinQuery? = if (StringUtils.isEmpty(prefix)) All else null
    }

    data class HasElement(val property: String, val value: Any) : GremlinQuery("he") {
        override fun traverse(g: YT): YT =
            g.where(
                `__`
                    .values<YTDBVertex, Any>(property)
                    .unfold<Any>()
                    .`is`(value)
            )

        override fun describeGremlin(s: StringBuilder): StringBuilder {
            return s.appendLine("where(")
                .append("values(").append(property).append(").unfold()").append("is(").append(value).appendLine(")")
                .appendLine(")")
        }

        override fun describe(s: StringBuilder): StringBuilder = s.append(property).append(" hasElement ").append(value)
    }

    data class HasLinkTo(val linkName: String, val rid: RID) : GremlinQuery("hlt") {
        override fun traverse(g: YT): YT =
            g.where(
                `__`
                    .out(YTDBVertexEntity.edgeClassName(linkName))
                    .hasId(rid)
            )
            .asYT()

        override fun describeGremlin(s: StringBuilder): StringBuilder {
            return s.appendLine("where(")
                .append("out(").append(linkName).appendLine(")")
                .append("hasId(").append(rid).appendLine(")")
                .appendLine(")")
        }

        override fun describe(s: StringBuilder): StringBuilder =
            s.append("hasLinkTo(").append(linkName).append(", ").append(rid).append(")")
    }

    data class HasLink(val linkName: String) : GremlinQuery("hl") {
        override fun traverse(g: YT): YT =
            g.where(`__`.out(YTDBVertexEntity.edgeClassName(linkName)))

        override fun describeGremlin(s: StringBuilder): StringBuilder {
            return s.appendLine("where(")
                .append("out(").append(linkName).appendLine(")")
                .appendLine(")")
        }

        override fun describe(s: StringBuilder): StringBuilder =
            s.append("hasLink(").append(linkName).append(")")
    }

    data class HasNoLink(val linkName: String) : GremlinQuery("hnl") {
        override fun traverse(g: YT): YT =
            g.not(`__`.out(YTDBVertexEntity.edgeClassName(linkName)))

        override fun describeGremlin(s: StringBuilder): StringBuilder {
            return s.appendLine("not(")
                .append("out(").append(linkName).appendLine(")")
                .appendLine(")")
        }

        override fun describe(s: StringBuilder): StringBuilder =
            s.append("hasNoLink(").append(linkName).append(")")
    }

    data class PropInRange(val propName: String, val min: Comparable<*>, val max: Comparable<*>) : GremlinQuery("pb") {
        override fun traverse(g: YT): YT =
            g.has(propName, P.gte(min).and(P.lte(max)))
        override fun describe(s: StringBuilder): StringBuilder = s.append(min).append(" <= ").append(propName).append(" <= ").append(max)
        override fun describeGremlin(s: StringBuilder): StringBuilder =
            s.append("has(").append(propName).append(", gte(").append(min).append(") and lte(").append(max).append("))")
    }

    enum class SortDirection {
        ASC, DESC
    }

    data class SortBy(val propName: String, val direction: SortDirection) : GremlinQuery("sb") {
        override fun traverse(g: YT): YT = g.order().by(
            propName, when (direction) {
                SortDirection.ASC -> Order.asc
                SortDirection.DESC -> Order.desc
            }
        )

        override fun describe(s: StringBuilder): StringBuilder =
            s.append(".sortBy(").append(propName).append(", ").append(direction).append(")")

        override fun describeGremlin(s: StringBuilder): StringBuilder =
            s.append(".order().by(").append(propName).append(", ").append(direction.name.lowercase()).append(")")
    }

    data class Union(val first: GremlinQuery, val second: GremlinQuery) : GremlinQuery("union") {
        override fun traverse(g: YT): YT = g.union(
            first.traverse(`__`.V<Any>().asYT()),
            second.traverse(`__`.V<Any>().asYT())
        )

        override fun describe(s: StringBuilder): StringBuilder {
            s.append("UNION(")
            first.describe(s).append(", ")
            return second.describe(s).append(")")
        }

        override fun describeGremlin(s: StringBuilder): StringBuilder {
            s.appendLine(".union(")
            first.describeGremlin(s.append("V()")).appendLine(",")
            second.describeGremlin(s.append("V()")).appendLine()
            s.appendLine(")")
            return s
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun GraphTraversal<*, *>.asYT(): YT = this as YT
}
