package jetbrains.exodus.query

import jetbrains.exodus.entitystore.Entity
import jetbrains.exodus.entitystore.youtrackdb.gremlin.GremlinEntityIterable
import jetbrains.exodus.entitystore.youtrackdb.gremlin.GremlinQuery
import jetbrains.exodus.query.metadata.ModelMetaData

class GremlinBinaryNode(
    private val left: NodeBase,
    private val right: NodeBase,
    commutative: Boolean,
    private val shortName: String,
    private val combineQuery: (GremlinQuery, GremlinQuery) -> GremlinQuery,
    private val combineInMem: ((Iterable<Entity>, Iterable<Entity>) -> Iterable<Entity>)? = null
) : BinaryOperator(left, right, commutative), GremlinNode {

    override fun getQuery(): GremlinQuery? {
        val leftQ = (left as? GremlinNode)?.getQuery()
        val rightQ = (right as? GremlinNode)?.getQuery()

        return if (leftQ == null || rightQ == null) null
        else combineQuery(leftQ, rightQ)
    }

    override fun instantiate(
        entityType: String,
        queryEngine: QueryEngine,
        metaData: ModelMetaData?,
        context: InstantiateContext?
    ): Iterable<Entity> =
        query?.let {
            GremlinEntityIterable.create(
                entityType,
                queryEngine.oStore.requireActiveTransaction(),
                it
            )
        } ?: combineInMem?.let {
            it(
                getLeft().instantiate(entityType, queryEngine, metaData, context),
                getRight().instantiate(entityType, queryEngine, metaData, context)
            )
        } ?: run {
            throw IllegalStateException("Only GremlinNode instances can be used in the chain")
        }

    override fun getClone(): NodeBase =
        GremlinBinaryNode(getLeft().clone, getRight().clone, commutative, shortName, combineQuery, combineInMem)

    override fun getSimpleName(): String = shortName
}
