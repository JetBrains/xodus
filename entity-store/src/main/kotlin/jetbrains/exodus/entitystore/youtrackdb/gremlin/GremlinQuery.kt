package jetbrains.exodus.entitystore.youtrackdb.gremlin

import com.jetbrains.youtrack.db.api.gremlin.YTDBVertex
import com.jetbrains.youtrack.db.api.record.RID
import jetbrains.exodus.entitystore.youtrackdb.YTDBVertexEntity
import org.apache.commons.lang3.StringUtils
import org.apache.tinkerpop.gremlin.process.traversal.Order
import org.apache.tinkerpop.gremlin.process.traversal.P
import org.apache.tinkerpop.gremlin.process.traversal.TextP
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__

typealias YT = GraphTraversal<*, YTDBVertex>

sealed class GremlinQuery {

    companion object {
        fun where(block: GremlinBlock): GremlinQuery = Condition(block)
    }

    protected data class YTBuilder(val traversal: YT, val counter: Int) {
        companion object {
            fun of(t: GraphTraversal<*, *>, block: GremlinBlock? = null, counter: Int = 0) = YTBuilder(
                block?.traverse(t.asYT()) ?: t.asYT(),
                counter
            )
        }

        fun combine(block: GremlinBlock) = YTBuilder(block.traverse(traversal), counter)
        fun combine(block: (YT) -> YT) = YTBuilder(block(traversal), counter)
    }

    fun start(gs: GraphTraversalSource): YT = startTraversal(gs).traversal

    protected abstract fun startTraversal(gs: GraphTraversalSource): YTBuilder
    protected abstract fun continueTraversal(t: YT, counter: Int): YTBuilder

    fun follow(direction: LinkDirection, linkName: String) = Traverse(this, direction, linkName)

    fun andThen(block: GremlinBlock): GremlinQuery = when (this) {
        is Condition -> Condition(this.block.andThen(block))
        is Labeled -> Labeled(this.inner.andThen(block), this.label)
        is AndThen -> AndThen(this.query, block.andThen(block))
        is UnionAll, is Traverse, is Aggregate -> AndThen(this, block.andThen(block))
    }

    private fun combineEfficient(
        other: GremlinQuery,
        condCombiner: (GremlinBlock, GremlinBlock) -> GremlinBlock
    ): GremlinQuery? {
        fun extractLabel(q: GremlinQuery): String? = if (q is Labeled) q.label else null
        fun extractCondition(q: GremlinQuery): GremlinBlock? = when (q) {
            is Condition -> q.block
            is Labeled -> extractCondition(q.inner)
            else -> null
        }

        val thisLabel = extractLabel(this)
        val otherLabel = extractLabel(other)

        if (thisLabel != null && otherLabel != null && thisLabel != otherLabel) {
            return null
        }

        val thisCondition = extractCondition(this)
        val otherCondition = extractCondition(other)

        if (thisCondition == null || otherCondition == null) {
            return null
        }

        val label = thisLabel ?: otherLabel
        val combinedCondition = Condition(condCombiner(thisCondition, otherCondition))

        return if (label != null) Labeled(combinedCondition, label) else combinedCondition
    }

    fun union(other: GremlinQuery): GremlinQuery =
        combineEfficient(other, GremlinBlock::Or)
            ?: this.unionAll(other).andThen(GremlinBlock.Dedup)

    fun intersect(other: GremlinQuery): GremlinQuery =
        combineEfficient(other, GremlinBlock::And)
            ?: Aggregate(this, other) { P.within(it) }

    fun difference(other: GremlinQuery): GremlinQuery =
        combineEfficient(other) { a, b -> GremlinBlock.And(a, GremlinBlock.Not(b)) }
            ?: Aggregate(this, other) { P.without(it) }

    fun labeled(label: String) = Labeled(this, label)

    fun unionAll(vararg queries: GremlinQuery) = UnionAll(listOf(this, *queries))

    data class Condition(
        val block: GremlinBlock
    ) : GremlinQuery() {

        override fun startTraversal(gs: GraphTraversalSource): YTBuilder = YTBuilder.of(gs.V(), block)
        override fun continueTraversal(t: YT, counter: Int): YTBuilder = YTBuilder.of(t.V(), block, counter)
    }

    sealed class Extended(
        private val _inner: GremlinQuery,
        private val _block: GremlinBlock
    ) : GremlinQuery() {

        override fun startTraversal(gs: GraphTraversalSource): YTBuilder = _inner.startTraversal(gs).combine(_block)
        override fun continueTraversal(t: YT, counter: Int): YTBuilder = _inner.continueTraversal(t, counter).combine(_block)
    }

    data class Labeled(
        val inner: GremlinQuery,
        val label: String,
    ) : Extended(inner, GremlinBlock.HasLabel(label))

    data class AndThen(
        val query: GremlinQuery,
        val block: GremlinBlock
    ) : Extended(query, block)

    enum class LinkDirection {
        IN, OUT
    }

    data class Traverse(
        val inner: GremlinQuery,
        val direction: LinkDirection,
        val linkName: String
    ) : Extended(
        inner,
        when (direction) {
            LinkDirection.IN -> GremlinBlock.InLink(linkName)
            LinkDirection.OUT -> GremlinBlock.OutLink(linkName)
        }
    )

    data class UnionAll(val subqueries: List<GremlinQuery>) : GremlinQuery() {

        private fun subtraversals(counter: Int): Pair<Array<YT>, Int> {
            val result = mutableListOf<YT>()
            var c = counter
            subqueries.forEach { sq ->
                val subRes = sq.continueTraversal(`__`.start(), c)
                c = subRes.counter
                result.add(subRes.traversal)
            }

            return Pair(result.toTypedArray(), c)
        }

        override fun startTraversal(gs: GraphTraversalSource): YTBuilder {
            val subi = subtraversals(0)
            return YTBuilder.of(gs.union(*subi.first), counter = subi.second)
        }

        override fun continueTraversal(t: YT, counter: Int): YTBuilder {
            val subi = subtraversals(0)
            return YTBuilder.of(t.union(*subi.first), counter = subi.second)
        }
    }

    data class Aggregate(val left: GremlinQuery, val right: GremlinQuery, val fn: (String) -> P<String>) :
        GremlinQuery() {

        override fun startTraversal(gs: GraphTraversalSource): YTBuilder = builder(right.startTraversal(gs))
        override fun continueTraversal(t: YT, counter: Int): YTBuilder = builder(right.continueTraversal(t, counter))

        private fun builder(rightInner: YTBuilder): YTBuilder {
            val rightSetName = "aggr_" + rightInner.counter

            return left
                .continueTraversal(
                    rightInner.traversal.aggregate(rightSetName),
                    rightInner.counter + 1
                )
                .combine { it.where(fn(rightSetName)) }
        }

    }
}

abstract class GremlinBlock(val shortName: String) {

    abstract fun traverse(g: YT): YT

    abstract fun describe(s: StringBuilder): StringBuilder

    abstract fun describeGremlin(s: StringBuilder): StringBuilder

    open fun simplify(): GremlinBlock? = null

    abstract class GremlinUnaryOp(shortName: String) : GremlinBlock(shortName)
    abstract class GremlinBinaryOp(shortName: String) : GremlinBlock(shortName)

    // TODO: not stack safe potentially?
    fun andThen(query: GremlinBlock) =
        if (this is All) query
        else if (query is All) this
        else AndThen(this, query)

    data class AndThen(val left: GremlinBlock, val right: GremlinBlock) : GremlinBinaryOp("andThen") {
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
        override fun simplify(): GremlinBlock? =
            when {
                left is All -> right
                right is All -> left
                else -> null
            }
    }

    data class Or(val left: GremlinBlock, val right: GremlinBlock) : GremlinBinaryOp("or") {
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
        override fun simplify(): GremlinBlock? =
            if (left is All || right is All) All
            else null
    }

    data class And(val left: GremlinBlock, val right: GremlinBlock) : GremlinBinaryOp("and") {
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
        override fun simplify(): GremlinBlock? =
            when {
                left is All -> right
                right is All -> left
                else -> null
            }
    }

    data class Not(val query: GremlinBlock) : GremlinUnaryOp("not") {
        override fun traverse(g: YT): YT = g.not(query.traverse(`__`.start<Any>().asYT()))
        override fun describeGremlin(s: StringBuilder): StringBuilder {
            s.appendLine(".not(")
            query.describeGremlin(s).appendLine()
            s.appendLine(")")
            return s
        }

        override fun describe(s: StringBuilder): StringBuilder = query.describe(s.append("NOT "))
        override fun simplify(): GremlinBlock? =
            when (query) {
                is Not -> query.query
                else -> null
            }
    }

    data object All : GremlinBlock("all") {
        override fun traverse(g: YT): YT = g
        override fun describe(s: StringBuilder): java.lang.StringBuilder = s.append("*")
        override fun describeGremlin(s: StringBuilder): StringBuilder {
            return s
        }
    }

    data object Dedup : GremlinBlock("dedup") {
        override fun traverse(g: YT): YT = g.dedup()
        override fun describe(s: StringBuilder): StringBuilder = s.append(".dedup()")
        override fun describeGremlin(s: StringBuilder): StringBuilder {
            return s.append(".dedup()")
        }
    }

    data class HasLabel(val entityType: String) : GremlinBlock("hl") {
        override fun traverse(g: YT): YT = g.hasLabel(entityType).asYT()
        override fun describe(s: StringBuilder): StringBuilder = s.append(".hasLabel(").append(entityType).append(")")
        override fun describeGremlin(s: StringBuilder): StringBuilder {
            return s.append(".hasLabel(").append(entityType).append(")")
        }
    }

    data class Limit(val limit: Long) : GremlinBlock("lim") {
        override fun traverse(g: YT): YT = g.limit(limit)
        override fun describe(s: StringBuilder): StringBuilder = s.append(".limit(").append(limit).append(")")
        override fun describeGremlin(s: StringBuilder): StringBuilder {
            return s.append(".limit(").append(limit).append(")")
        }
    }

    data class Skip(val skip: Long) : GremlinBlock("skp") {
        override fun traverse(g: YT): YT = g.skip(skip)
        override fun describe(s: StringBuilder): StringBuilder = s.append(".skip(").append(skip).append(")")
        override fun describeGremlin(s: StringBuilder): StringBuilder {
            return s.append(".skip(").append(skip).append(")")
        }
    }

    data object Tail : GremlinBlock("tail") {
        override fun traverse(g: YT): YT = g.tail()
        override fun describe(s: StringBuilder): StringBuilder = s.append(".tail()")
        override fun describeGremlin(s: StringBuilder): StringBuilder {
            return s.append(".tail()")
        }
    }

    data class PropEqual(val property: String, val value: Any?) : GremlinBlock("eq") {
        override fun traverse(g: YT): YT = g.has(property, value)
        override fun describe(s: StringBuilder): StringBuilder = s.append(property).append("=").append(value)
        override fun describeGremlin(s: StringBuilder): StringBuilder {
            return s.append("has(").append(property).append(", ").append(value).append(")")
        }
    }

    data class PropNull(val property: String) : GremlinBlock("nul") {
        override fun traverse(g: YT): YT = g.hasNot(property)
        override fun describe(s: StringBuilder): StringBuilder = s.append(property).append(" IS NULL")
        override fun describeGremlin(s: StringBuilder): StringBuilder {
            return s.append("hasNot(").append(property).append(")")
        }
    }

    data class PropNotNull(val property: String) : GremlinBlock("nn") {
        override fun traverse(g: YT): YT = g.has(property)
        override fun describe(s: StringBuilder): StringBuilder = s.append(property).append(" IS NOT NULL")
        override fun describeGremlin(s: StringBuilder): StringBuilder {
            return s.append("has(").append(property).append(")")
        }
    }

    data object Reverse : GremlinBlock("rev") {
        override fun traverse(g: YT): YT = g.reverse()
        override fun describe(s: StringBuilder): StringBuilder = s.append(".reverse()")
        override fun describeGremlin(s: StringBuilder): StringBuilder {
            return s.append(".reverse()")
        }
    }

    data class OutLink(val linkName: String) : GremlinBlock("olnk") {
        override fun traverse(g: YT): YT = g.out(YTDBVertexEntity.edgeClassName(linkName)).asYT()
        override fun describe(s: StringBuilder): StringBuilder = s.append(".out(").append(linkName).append(")")
        override fun describeGremlin(s: StringBuilder): StringBuilder = s.append(".out(").append(linkName).append(")")
    }

    data class InLink(val linkName: String) : GremlinBlock("ilnk") {
        override fun traverse(g: YT): YT = g.`in`(YTDBVertexEntity.edgeClassName(linkName)).asYT()
        override fun describe(s: StringBuilder): StringBuilder = s.append(".in(").append(linkName).append(")")
        override fun describeGremlin(s: StringBuilder): StringBuilder = s.append(".in(").append(linkName).append(")")
    }

    data class HasSubstring(val property: String, val substring: String?, val caseSensitive: Boolean) :
        GremlinBlock("hsub") {
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

        override fun simplify(): GremlinBlock? = if (StringUtils.isEmpty(substring)) All else null
    }

    data class HasPrefix(val property: String, val prefix: String, val caseSensitive: Boolean) : GremlinBlock("hp") {
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

        override fun simplify(): GremlinBlock? = if (StringUtils.isEmpty(prefix)) All else null
    }

    data class HasElement(val property: String, val value: Any) : GremlinBlock("he") {
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

    data class HasLinkTo(val linkName: String, val rid: RID) : GremlinBlock("hlt") {
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

    data class HasLink(val linkName: String) : GremlinBlock("hl") {
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

    data class HasNoLink(val linkName: String) : GremlinBlock("hnl") {
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

    data class PropInRange(val propName: String, val min: Comparable<*>, val max: Comparable<*>) : GremlinBlock("pb") {
        override fun traverse(g: YT): YT =
            g.has(propName, P.gte(min).and(P.lte(max)))
        override fun describe(s: StringBuilder): StringBuilder = s.append(min).append(" <= ").append(propName).append(" <= ").append(max)
        override fun describeGremlin(s: StringBuilder): StringBuilder =
            s.append("has(").append(propName).append(", gte(").append(min).append(") and lte(").append(max).append("))")
    }

    enum class SortDirection {
        ASC, DESC
    }

    data class SortBy(val propName: String, val direction: SortDirection) : GremlinBlock("sb") {
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

    data class SortByLinked(val linkName: String, val propName: String, val direction: SortDirection) :
        GremlinBlock("sbl") {
        override fun traverse(g: YT): YT = g.order().by(
            `__`.out(YTDBVertexEntity.edgeClassName(linkName)).values<Any>(propName),
            when (direction) {
                SortDirection.ASC -> Order.asc
                SortDirection.DESC -> Order.desc
            }
        )

        override fun describe(s: StringBuilder): StringBuilder {
            TODO("Not yet implemented")
        }

        override fun describeGremlin(s: StringBuilder): StringBuilder {
            TODO("Not yet implemented")
        }
    }

    data class Union(val first: GremlinBlock, val second: GremlinBlock) : GremlinBlock("union") {
        override fun traverse(g: YT): YT = g.union(
            traverseInner(first),
            traverseInner(second)
        )

        private fun traverseInner(inner: GremlinBlock): YT = inner.traverse(`__`.start<Any>().asYT())

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
}

@Suppress("UNCHECKED_CAST")
fun GraphTraversal<*, *>.asYT(): YT = this as YT
