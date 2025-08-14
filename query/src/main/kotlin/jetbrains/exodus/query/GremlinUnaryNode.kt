package jetbrains.exodus.query

import jetbrains.exodus.entitystore.Entity
import jetbrains.exodus.entitystore.youtrackdb.gremlin.GremlinEntityIterable
import jetbrains.exodus.entitystore.youtrackdb.gremlin.GremlinBlock
import jetbrains.exodus.query.metadata.ModelMetaData

class GremlinUnaryNode(
    child: NodeBase,
    val shortName: String,
    val op: (GremlinBlock) -> GremlinBlock
) : UnaryNode(child), GremlinNode {

    override fun getBlock(): GremlinBlock? =
        (child as? GremlinNode)?.block?.let { op(it) }

    override fun instantiate(
        entityType: String,
        queryEngine: QueryEngine,
        metaData: ModelMetaData?,
        context: InstantiateContext?
    ): Iterable<Entity> {
        val newQuery = block ?: run {
            throw IllegalStateException("Only GremlinNode instances can be used in the chain")
        }

        // todo: We should operate with GremlinQueries, not blocks at this level
        return GremlinEntityIterable.where(
            entityType,
            queryEngine.oStore.requireActiveTransaction(),
            newQuery
        )
    }

    override fun getClone(): NodeBase = GremlinUnaryNode(child.clone, shortName, op)

    override fun getSimpleName(): String = shortName
}