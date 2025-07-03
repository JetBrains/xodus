package jetbrains.exodus.query

import jetbrains.exodus.entitystore.Entity
import jetbrains.exodus.entitystore.youtrackdb.gremlin.GremlinEntityIterable
import jetbrains.exodus.entitystore.youtrackdb.gremlin.GremlinQuery
import jetbrains.exodus.query.metadata.ModelMetaData

class GremlinUnaryNode(
    child: NodeBase,
    val shortName: String,
    val op: (GremlinQuery) -> GremlinQuery
) : UnaryNode(child), GremlinNode {

    override fun getQuery(): GremlinQuery? =
        (child as? GremlinNode)?.query?.let { op(it) }

    override fun instantiate(
        entityType: String,
        queryEngine: QueryEngine,
        metaData: ModelMetaData?,
        context: InstantiateContext?
    ): Iterable<Entity> {
        val newQuery = query ?: run {
            throw IllegalStateException("Only GremlinNode instances can be used in the chain")
        }

        return GremlinEntityIterable.create(
            entityType,
            queryEngine.oStore.requireActiveTransaction(),
            newQuery
        )
    }

    override fun getClone(): NodeBase = GremlinUnaryNode(child.clone, shortName, op)

    override fun getSimpleName(): String = shortName
}