package jetbrains.exodus.query.metadata

fun Iterable<EntityMetaData>.sortedTopologically(): List<EntityMetaData> {
    val adj = HashMap<String, HashSet<String>>()
    val metaDataByName = HashMap<String, EntityMetaData>()
    for (entity in this) {
        require(!metaDataByName.containsKey(entity.type)) { "Two EntityMetaData instances with the same type=${entity.type} found. Happy debugging!" }

        metaDataByName[entity.type] = entity
        val superclass = entity.superType
        val subclass = entity.type
        if (superclass != null) {
            adj.getOrPut(superclass) { HashSet() }.add(subclass)
        }
    }

    val visited = HashSet<String>()
    val visiting = HashSet<String>()
    val result = ArrayDeque<String>()

    fun postOrderTraversal(node: String) {
        require(node !in visiting) { "Cycle detected involving type=$node. visiting set: ${visiting.joinToString(", ")}" }
        if (node in visited) return

        visiting.add(node)

        for (subclass in adj.getOrDefault(node, emptySet())) {
            postOrderTraversal(subclass)
        }

        result.addFirst(node)
        visiting.remove(node)
        visited.add(node)
    }

    for (entity in this) {
        postOrderTraversal(entity.type)
    }

    return result.map { className ->
        require(className in metaDataByName) { "$className type is in the result set but there is no such a type among the original list of EntityMetaData" }
        metaDataByName.getValue(className)
    }
}