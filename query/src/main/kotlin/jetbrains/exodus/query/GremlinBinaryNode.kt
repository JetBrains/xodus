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
    private val combineInMem: (Iterable<Entity>, Iterable<Entity>) -> Iterable<Entity>
) : BinaryOperator(left, right, commutative), GremlinNode {

    override fun getQuery(): GremlinQuery? =
        if (left is GremlinNode && right is GremlinNode)
            combineQuery(left.getQuery()!!, right.getQuery()!!)
        else null

    override fun instantiate(
        entityType: String,
        queryEngine: QueryEngine,
        metaData: ModelMetaData?,
        context: InstantiateContext?
    ): Iterable<Entity> =

        getQuery()?.let {
            GremlinEntityIterable.create(
                entityType,
                queryEngine.oStore.requireActiveTransaction(),
                it
            )
        } ?: combineInMem(
            getLeft().instantiate(entityType, queryEngine, metaData, context),
            getRight().instantiate(entityType, queryEngine, metaData, context)
        )

    override fun getClone(): NodeBase =
        GremlinBinaryNode(getLeft(), getRight(), commutative, shortName, combineQuery, combineInMem)

    override fun getSimpleName(): String = shortName
}
