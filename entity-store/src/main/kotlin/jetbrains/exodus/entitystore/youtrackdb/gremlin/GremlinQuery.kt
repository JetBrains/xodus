/*
 * Copyright ${inceptionYear} - ${year} ${owner}
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.entitystore.youtrackdb.gremlin

import com.jetbrains.youtrack.db.api.gremlin.YTDBVertex
import com.jetbrains.youtrack.db.api.record.RID
import jetbrains.exodus.entitystore.youtrackdb.YTDBVertexEntity
import jetbrains.exodus.entitystore.youtrackdb.gremlin.GremlinBlock.SortDirection
import org.apache.commons.lang3.StringUtils
import org.apache.tinkerpop.gremlin.process.traversal.Order
import org.apache.tinkerpop.gremlin.process.traversal.P
import org.apache.tinkerpop.gremlin.process.traversal.Scope
import org.apache.tinkerpop.gremlin.process.traversal.TextP
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__

typealias YT = GraphTraversal<*, YTDBVertex>

sealed class GremlinQuery {

    companion object {
        @JvmStatic
        val all = Where(GremlinBlock.All)
    }

    fun then(block: GremlinBlock): GremlinQuery = when {
        block is GremlinBlock.All -> this
        block.type == BlockType.SLICE -> Slice.of(this, block)
        block is GremlinBlock.Sort -> SortBy.of(this, block)
        block.type == BlockType.ORDER -> Order.of(this, block)
        block is GremlinBlock.HasLabel -> Labeled.of(this, block.entityType)
        block is GremlinBlock.InLink -> FollowLink(this, LinkDirection.IN, block.linkName)
        block is GremlinBlock.OutLink -> FollowLink(this, LinkDirection.OUT, block.linkName)

        block is GremlinBlock.AndThen -> throw IllegalArgumentException("Nested andThen is not allowed")
        block is GremlinBlock.Reverse -> when (this) {
            is SortBy -> this.reverseOrder()
            is ReversedOrder -> this.inner
            else -> ReversedOrder(this)
        }

        else -> when (this) {
            is Where -> Where(this.block.andThen(block))
            is ByIds -> Where(this.asBlock().andThen(block))
            is Labeled -> Labeled(this.inner.then(block), this.label)
            is AndThen -> AndThen(this.inner, this.block.andThen(block))
            else -> AndThen(this, block)
        }
    }

    fun start(gs: GraphTraversalSource): YT = startTraversal(gs).traversal

    abstract fun shortName(): String

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

    protected abstract fun startTraversal(gs: GraphTraversalSource): YTBuilder
    protected abstract fun continueTraversal(t: YT, paramCounter: Int, ignoreSort: Boolean): YTBuilder

    sealed class ConditionCombiner(
        val combineBlocks: (GremlinBlock, GremlinBlock) -> GremlinBlock,
        val combineIds: (List<RID>, List<RID>) -> List<RID>
    ) {
        data object Intersect : ConditionCombiner(
            combineBlocks = { a, b ->
                when {
                    a is GremlinBlock.None || b is GremlinBlock.None -> GremlinBlock.None
                    a is GremlinBlock.All -> b
                    b is GremlinBlock.All -> a
                    else -> GremlinBlock.And(a, b)
                }
            },
            combineIds = { a, b -> a.filter(b::contains) }
        )

        data object Union : ConditionCombiner(
            combineBlocks = { a, b ->
                when {
                    a is GremlinBlock.None -> b
                    b is GremlinBlock.None -> a
                    a is GremlinBlock.All || b is GremlinBlock.All -> GremlinBlock.All
                    else -> GremlinBlock.Or(a, b)
                }
            },
            combineIds = { a, b -> a + b.filter { !a.contains(it) } }
        )

        data object Difference : ConditionCombiner(
            combineBlocks = { a, b ->
                when {
                    a is GremlinBlock.None -> GremlinBlock.None
                    a is GremlinBlock.All -> GremlinBlock.Not(b)
                    b is GremlinBlock.All -> GremlinBlock.None
                    b is GremlinBlock.None -> a
                    else -> GremlinBlock.And(a, GremlinBlock.Not(b))
                }
            },
            combineIds = { a, b -> a.filter { !b.contains(it) } }
        )

    }

    // todo: handle SortBy here too
    private fun combineEfficient(
        other: GremlinQuery,
        condCombiner: ConditionCombiner
    ): GremlinQuery? {
        fun extractLabel(q: GremlinQuery): String? = if (q is Labeled) q.label else null
        fun extractCondition(q: GremlinQuery): GremlinBlock? = when (q) {
            is Labeled -> extractCondition(q.inner)
            is Condition -> q.asBlock()
            else -> null
        }

        val thisLabel = extractLabel(this)
        val otherLabel = extractLabel(other)

        if (thisLabel != null && otherLabel != null && thisLabel != otherLabel) {
            return null
        }

        if (this is ByIds && other is ByIds) {
            return ByIds(condCombiner.combineIds(this.ids, other.ids))
        }

        val thisCondition = extractCondition(this)
        val otherCondition = extractCondition(other)

        if (thisCondition == null || otherCondition == null) {
            return null
        }

        val label = thisLabel ?: otherLabel
        val combinedCondition = Where.of(condCombiner.combineBlocks(thisCondition, otherCondition))

        return if (label != null) Labeled.of(combinedCondition, label) else combinedCondition
    }

    fun union(other: GremlinQuery): GremlinQuery =
        combineEfficient(other, ConditionCombiner.Union)
            ?: this.unionAll(other).then(GremlinBlock.Dedup)

    fun intersect(other: GremlinQuery): GremlinQuery =
        combineEfficient(other, ConditionCombiner.Intersect)
            ?: Aggregate(this, other) { P.within(it) }

    fun difference(other: GremlinQuery): GremlinQuery =
        combineEfficient(other, ConditionCombiner.Difference)
            ?: Aggregate(this, other) { P.without(it) }

    fun unionAll(vararg queries: GremlinQuery) = UnionAll(listOf(this, *queries))

    sealed class Condition(private val _block: GremlinBlock) : GremlinQuery() {
        override fun startTraversal(gs: GraphTraversalSource): YTBuilder = YTBuilder.of(gs.V(), _block)
        override fun continueTraversal(t: YT, paramCounter: Int, ignoreSort: Boolean): YTBuilder =
            YTBuilder.of(t.V(), _block, paramCounter)

        override fun shortName(): String = _block.shortName

        fun combineBinary(other: Condition, combiner: (GremlinBlock, GremlinBlock) -> GremlinBlock): Condition =
            Where.of(combiner(_block, other._block))

        fun combineUnary(combiner: (GremlinBlock) -> GremlinBlock): Condition =
            Where.of(combiner(_block))

        fun asBlock() = _block
    }

    data class Where(val block: GremlinBlock) : Condition(block) {

        companion object {
            fun of(block: GremlinBlock): Where {
                require(block.type == BlockType.CONDITION || block.type == BlockType.COMBINE)
                return Where(block)
            }
        }
    }

    // todo: think how to preserve the order of the parameters
    // todo: handle Take & Skip differently too
    data class ByIds(val ids: List<RID>) : Condition(GremlinBlock.IdWithin(ids))

    data class NestedCondition(val structure: List<String>, val condition: Condition) : Condition(
        GremlinBlock.Where(
            structure
                .fold(GremlinBlock.All as GremlinBlock) { a, b ->
                    a.andThen(GremlinBlock.OutLink(b))
                }
                .andThen(condition.asBlock())
        )
    )

    sealed class Chained(
        private val _inner: GremlinQuery,
        private val _block: GremlinBlock,
        private val dependsOnOrder: Boolean = false,
        private val isOrder: Boolean = false
    ) : GremlinQuery() {

        override fun startTraversal(gs: GraphTraversalSource): YTBuilder =
            _inner.startTraversal(gs).combine(_block)

        override fun continueTraversal(t: YT, paramCounter: Int, ignoreSort: Boolean): YTBuilder =
            _inner
                .continueTraversal(t, paramCounter, ignoreSort && !dependsOnOrder)
                // todo: this optimization obviously brings some errors
                // .combine(if (isOrder && ignoreSort) GremlinBlock.All else _block)
                .combine(_block)

        override fun shortName(): String = _block.shortName
    }

    data class Labeled(val inner: GremlinQuery, val label: String) :
        Chained(inner, GremlinBlock.HasLabel(label)) {
        companion object {
            fun of(query: GremlinQuery, label: String): GremlinQuery =
                Labeled((query as? Labeled)?.inner ?: query, label)
        }
    }

    data class AndThen(val inner: GremlinQuery, val block: GremlinBlock) :
        Chained(inner, block)

    data class Slice(
        val inner: GremlinQuery,
        val sliceBlock: GremlinBlock,
    ) : Chained(inner, sliceBlock, dependsOnOrder = true) {

        companion object {
            fun of(query: GremlinQuery, sliceBlock: GremlinBlock): GremlinQuery {
                require(sliceBlock.type == BlockType.SLICE)
                return Slice(
                    inner = (query as? Slice)?.inner ?: query,
                    sliceBlock = ((query as? Slice)?.sliceBlock ?: GremlinBlock.All).andThen(sliceBlock)
                )
            }
        }
    }

    enum class LinkDirection {
        IN, OUT
    }

    data class FollowLink(
        val inner: GremlinQuery,
        val direction: LinkDirection,
        val linkName: String
    ) : Chained(
        inner,
        when (direction) {
            LinkDirection.IN -> GremlinBlock.InLink(linkName)
            LinkDirection.OUT -> GremlinBlock.OutLink(linkName)
        },
    )

    data class UnionAll(val subqueries: List<GremlinQuery>) : GremlinQuery() {

        private fun subtraversals(counter: Int, sortApplied: Boolean): Pair<Array<YT>, Int> {
            val result = mutableListOf<YT>()
            var c = counter
            subqueries.forEach { sq ->
                val subRes = sq.continueTraversal(`__`.start(), c, sortApplied)
                c = subRes.counter
                result.add(subRes.traversal)
            }

            return Pair(result.toTypedArray(), c)
        }

        override fun startTraversal(gs: GraphTraversalSource): YTBuilder {
            val subi = subtraversals(0, sortApplied = false)
            return YTBuilder.of(gs.union(*subi.first), counter = subi.second)
        }

        override fun continueTraversal(t: YT, paramCounter: Int, ignoreSort: Boolean): YTBuilder {
            val subi = subtraversals(0, ignoreSort)
            return YTBuilder.of(t.union(*subi.first), counter = subi.second)
        }

        override fun shortName(): String = "unionAll"
    }

    data class Aggregate(val left: GremlinQuery, val right: GremlinQuery, val fn: (String) -> P<String>) :
        GremlinQuery() {

        override fun startTraversal(gs: GraphTraversalSource): YTBuilder =
            builder(right.startTraversal(gs), ignoreSort = false)

        // we don't need sorting for the right part
        override fun continueTraversal(t: YT, paramCounter: Int, ignoreSort: Boolean): YTBuilder =
            builder(right.continueTraversal(t, paramCounter, ignoreSort = true), ignoreSort)

        private fun builder(rightInner: YTBuilder, ignoreSort: Boolean): YTBuilder {
            val rightSetName = "aggr_" + rightInner.counter

            return left
                .continueTraversal(
                    rightInner.traversal.aggregate(rightSetName).fold().asYT(),
                    rightInner.counter + 1,
                    ignoreSort = ignoreSort
                )
                .combine { it.where(fn(rightSetName)) }
        }

        override fun shortName(): String = "aggregate"
    }

    data class SortBy(val inner: GremlinQuery, val sortBlock: GremlinBlock.Sort) :
        Chained(inner, sortBlock, dependsOnOrder = false, isOrder = true) {
        companion object {
            fun of(query: GremlinQuery, sortBlock: GremlinBlock.Sort): GremlinQuery = SortBy(query, sortBlock)
        }

        fun reverseOrder(): SortBy = this.copy(
            inner = inner, sortBlock = sortBlock.copy(
                by = sortBlock.by,
                direction = if (sortBlock.direction == SortDirection.ASC) SortDirection.DESC else SortDirection.ASC
            )
        )
    }

    data class ReversedOrder(val inner: GremlinQuery) : Chained(inner, GremlinBlock.Reverse)

    data class Order(val inner: GremlinQuery, val orderBlock: GremlinBlock) :
        Chained(inner, orderBlock, isOrder = true) {

        companion object {
            fun of(query: GremlinQuery, orderBlock: GremlinBlock): GremlinQuery {
                require(orderBlock.type == BlockType.ORDER)
                return Order(
                    inner = (query as? Order)?.inner ?: query,
                    orderBlock = ((query as? Order)?.orderBlock ?: GremlinBlock.All).andThen(orderBlock)
                )
            }
        }
    }
}

enum class BlockType {
    SLICE,
    ORDER,
    LINK,
    CONDITION,
    COMBINE,
    COMPOSE
}

sealed class GremlinBlock(val shortName: String, val type: BlockType) {

    abstract fun traverse(g: YT): YT

    abstract fun describe(s: StringBuilder): StringBuilder

    abstract fun describeGremlin(s: StringBuilder): StringBuilder

    open fun simplify(): GremlinBlock? = null

    // TODO: not stack safe potentially?
    fun andThen(query: GremlinBlock) =
        if (this is All) query
        else if (query is All) this
        else AndThen(this, query)

    // todo: make this private, don't expose it to the outside. users should
    data class AndThen(val left: GremlinBlock, val right: GremlinBlock) : GremlinBlock("andThen", BlockType.COMPOSE) {
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

    data class Or(val left: GremlinBlock, val right: GremlinBlock) : GremlinBlock("or", BlockType.COMBINE) {
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

    data class Where(val inner: GremlinBlock) : GremlinBlock("where", BlockType.CONDITION) {
        override fun traverse(g: YT): YT = g.where(inner.traverse(`__`.start<Any>().asYT()))
        override fun describe(s: StringBuilder): StringBuilder = inner.describe(s.append("WHERE "))

        override fun describeGremlin(s: StringBuilder): StringBuilder =
            inner.describeGremlin(s.append(".where(")).append(")")
    }

    data class And(val left: GremlinBlock, val right: GremlinBlock) : GremlinBlock("and", BlockType.COMBINE) {
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

    data class Not(val query: GremlinBlock) : GremlinBlock("not", BlockType.COMBINE) {
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

    data object All : GremlinBlock("all", BlockType.CONDITION) {
        override fun traverse(g: YT): YT = g
        override fun describe(s: StringBuilder): java.lang.StringBuilder = s.append("*")
        override fun describeGremlin(s: StringBuilder): StringBuilder {
            return s
        }
    }

    data object None : GremlinBlock("none", BlockType.CONDITION) {
        override fun traverse(g: YT): YT = g.where(`__`.constant(false).asYT())

        override fun describe(s: StringBuilder): StringBuilder = s.append("none")

        override fun describeGremlin(s: StringBuilder): StringBuilder = s.append(".where(__.constant(false))")
    }

    data object Dedup : GremlinBlock("dedup", BlockType.ORDER) {
        override fun traverse(g: YT): YT = g.dedup()
        override fun describe(s: StringBuilder): StringBuilder = s.append(".dedup()")
        override fun describeGremlin(s: StringBuilder): StringBuilder {
            return s.append(".dedup()")
        }
    }

    data class HasLabel(val entityType: String) : GremlinBlock("hl", BlockType.CONDITION) {
        override fun traverse(g: YT): YT = g.hasLabel(entityType).asYT()
        override fun describe(s: StringBuilder): StringBuilder = s.append(".hasLabel(").append(entityType).append(")")
        override fun describeGremlin(s: StringBuilder): StringBuilder =
            s.append(".hasLabel(").append(entityType).append(")")
    }

    data class Limit(val limit: Long) : GremlinBlock("lim", BlockType.SLICE) {
        init {
            require(limit > 0) { "Limit must be positive" }
        }
        override fun traverse(g: YT): YT = g.limit(limit)
        override fun describe(s: StringBuilder): StringBuilder = s.append(".limit(").append(limit).append(")")
        override fun describeGremlin(s: StringBuilder): StringBuilder = s.append(".limit(").append(limit).append(")")
    }

    data class Skip(val skip: Long) : GremlinBlock("skp", BlockType.SLICE) {
        init {
            require(skip >= 0) { "Skip must be non-negative" }
        }
        override fun traverse(g: YT): YT = g.skip(skip)
        override fun describe(s: StringBuilder): StringBuilder = s.append(".skip(").append(skip).append(")")
        override fun describeGremlin(s: StringBuilder): StringBuilder {
            return s.append(".skip(").append(skip).append(")")
        }
    }

    data class Tail(val tail: Long) : GremlinBlock("tail", BlockType.SLICE) {
        init {
            require(tail >= 0) { "Skip must be non-negative" }
        }

        override fun traverse(g: YT): YT = g.tail(tail)
        override fun describe(s: StringBuilder): StringBuilder = s.append(".tail(").append(tail).append(")")
        override fun describeGremlin(s: StringBuilder): StringBuilder = s.append(".tail(").append(tail).append(")")
    }

    data class PropEqual(val property: String, val value: Any?) : GremlinBlock("eq", BlockType.CONDITION) {
        override fun traverse(g: YT): YT = g.has(property, value)
        override fun describe(s: StringBuilder): StringBuilder = s.append(property).append("=").append(value)
        override fun describeGremlin(s: StringBuilder): StringBuilder {
            return s.append("has(").append(property).append(", ").append(value).append(")")
        }
    }

    data class PropNull(val property: String) : GremlinBlock("nul", BlockType.CONDITION) {
        override fun traverse(g: YT): YT = g.hasNot(property)
        override fun describe(s: StringBuilder): StringBuilder = s.append(property).append(" IS NULL")
        override fun describeGremlin(s: StringBuilder): StringBuilder {
            return s.append("hasNot(").append(property).append(")")
        }
    }

    data class PropNotNull(val property: String) : GremlinBlock("nn", BlockType.CONDITION) {
        override fun traverse(g: YT): YT = g.has(property)
        override fun describe(s: StringBuilder): StringBuilder = s.append(property).append(" IS NOT NULL")
        override fun describeGremlin(s: StringBuilder): StringBuilder {
            return s.append("has(").append(property).append(")")
        }
    }

    data class OutLink(val linkName: String) : GremlinBlock("olnk", BlockType.LINK) {
        override fun traverse(g: YT): YT = g.out(YTDBVertexEntity.edgeClassName(linkName)).asYT()
        override fun describe(s: StringBuilder): StringBuilder = s.append(".out(").append(linkName).append(")")
        override fun describeGremlin(s: StringBuilder): StringBuilder = s.append(".out(").append(linkName).append(")")
    }

    data class InLink(val linkName: String) : GremlinBlock("ilnk", BlockType.LINK) {
        override fun traverse(g: YT): YT = g.`in`(YTDBVertexEntity.edgeClassName(linkName)).asYT()
        override fun describe(s: StringBuilder): StringBuilder = s.append(".in(").append(linkName).append(")")
        override fun describeGremlin(s: StringBuilder): StringBuilder = s.append(".in(").append(linkName).append(")")
    }

    data class HasSubstring(val property: String, val substring: String?, val caseSensitive: Boolean) :
        GremlinBlock("hsub", BlockType.CONDITION) {
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

    data class HasPrefix(val property: String, val prefix: String, val caseSensitive: Boolean) :
        GremlinBlock("hp", BlockType.CONDITION) {
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

    data class HasElement(val property: String, val value: Any) : GremlinBlock("he", BlockType.CONDITION) {
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

    data class HasLinkTo(val linkName: String, val rid: RID) : GremlinBlock("hlt", BlockType.CONDITION) {
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

    data class HasLink(val linkName: String) : GremlinBlock("hl", BlockType.CONDITION) {
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

    data class HasNoLink(val linkName: String) : GremlinBlock("hnl", BlockType.CONDITION) {
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

    data class PropInRange(val propName: String, val min: Comparable<*>, val max: Comparable<*>) :
        GremlinBlock("pb", BlockType.CONDITION) {
        override fun traverse(g: YT): YT =
            g.has(propName, P.gte(min).and(P.lte(max)))
        override fun describe(s: StringBuilder): StringBuilder = s.append(min).append(" <= ").append(propName).append(" <= ").append(max)
        override fun describeGremlin(s: StringBuilder): StringBuilder =
            s.append("has(").append(propName).append(", gte(").append(min).append(") and lte(").append(max).append("))")
    }

    data class PropWithin(val propName: String, val within: Collection<*>) : GremlinBlock("pw", BlockType.CONDITION) {
        override fun traverse(g: YT): YT =
            g.has(propName, P.within<Any>(within))

        override fun describe(s: StringBuilder): StringBuilder =
            s.append(propName).append(" within ").append(within)

        override fun describeGremlin(s: StringBuilder): StringBuilder =
            s.append("has(").append(propName).append(", within(").append(within).append("))")
    }

    data class IdEqual(val rid: RID) : GremlinBlock("ide", BlockType.CONDITION) {
        override fun traverse(g: YT): YT =
            g.hasId(rid)

        override fun describe(s: StringBuilder): StringBuilder =
            s.append("id=").append(rid)

        override fun describeGremlin(s: StringBuilder): StringBuilder =
            s.append("hasId(").append(rid).append(")")
    }

    data class IdWithin(val within: Collection<RID>) : GremlinBlock("idw", BlockType.CONDITION) {
        override fun traverse(g: YT): YT = g.hasId(P.within(within))

        override fun describe(s: StringBuilder): StringBuilder = s.append("id within ").append(within)

        override fun describeGremlin(s: StringBuilder): StringBuilder =
            s.append("hasId(within(").append(within).append("))")
    }

    enum class SortDirection {
        ASC, DESC
    }


    data class Sort(val by: By, val direction: SortDirection) : GremlinBlock("sb", BlockType.ORDER) {
        sealed interface By
        class ByProp(val propName: String) : By
        class ByLinked(val linkName: String, val propName: String) : By

        override fun traverse(g: YT): YT {
            val order = when (direction) {
                SortDirection.ASC -> Order.asc
                SortDirection.DESC -> Order.desc
            }

            return when (by) {
                is ByProp -> g.order().by(by.propName, order)
                is ByLinked -> g.order().by(
                    `__`.out(YTDBVertexEntity.edgeClassName(by.linkName)).values<Any>(by.propName),
                    order
                )
            }
        }

        override fun describe(s: StringBuilder): StringBuilder =
            s.append(".sortBy(").append(by).append(", ").append(direction).append(")")

        override fun describeGremlin(s: StringBuilder): StringBuilder =
            s.append(".order().by(").append(by).append(", ").append(direction.name.lowercase()).append(")")
    }

    data object Reverse : GremlinBlock("rev", BlockType.ORDER) {
        override fun traverse(g: YT): YT = g
            .fold().index<Any>().unfold<Any>()
            .order().by(`__`.tail<Any>(Scope.local, 1), Order.desc)
            .limit<Any>(Scope.local, 1)
            .asYT()

        override fun describe(s: StringBuilder): StringBuilder {
            TODO("Not yet implemented")
        }

        override fun describeGremlin(s: StringBuilder): StringBuilder {
            TODO("Not yet implemented")
        }
    }
}

@Suppress("UNCHECKED_CAST")
fun GraphTraversal<*, *>.asYT(): YT = this as YT
