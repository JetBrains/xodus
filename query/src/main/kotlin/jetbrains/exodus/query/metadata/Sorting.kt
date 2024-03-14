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
package jetbrains.exodus.query.metadata

fun Iterable<EntityMetaData>.sortedTopologically(): List<EntityMetaData> {
    val metaDataByName = HashMap<String, EntityMetaData>()
    for (entity in this) {
        require(!metaDataByName.containsKey(entity.type)) { "Two EntityMetaData instances with the same type=${entity.type} found. Happy debugging!" }
        metaDataByName[entity.type] = entity
    }

    val adj = HashMap<String, HashSet<String>>()
    for (entity in this) {
        val superclass = entity.superType
        val subclass = entity.type

        if (superclass != null) {
            require(metaDataByName.containsKey(superclass)) { "$subclass has superclass $superclass that is missing. Happy debugging!" }
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
        metaDataByName.getValue(className)
    }
}