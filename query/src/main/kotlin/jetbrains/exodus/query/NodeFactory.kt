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
package jetbrains.exodus.query

import jetbrains.exodus.entitystore.Entity
import jetbrains.exodus.entitystore.youtrackdb.YTDBEntityId
import jetbrains.exodus.entitystore.youtrackdb.gremlin.GremlinBlock
import jetbrains.exodus.entitystore.youtrackdb.gremlin.GremlinBlock.*
import jetbrains.exodus.entitystore.youtrackdb.gremlin.GremlinBlock.Or
import java.util.stream.Collectors
import java.util.stream.StreamSupport


object NodeFactory {
    @JvmStatic
    fun all_old(): NodeBase = GetAll()

    fun all(): GremlinLeaf = GremlinLeaf(All)

    fun propEqual(property: String, value: Comparable<*>?): GremlinLeaf =
        GremlinLeaf(
            if (value == null) PropNull((property))
            else PropEqual(property, value)
        )

    fun propNull(property: String): GremlinLeaf =
        GremlinLeaf(PropNull(property))

    fun propNotNull(property: String): GremlinLeaf =
        GremlinLeaf(PropNotNull(property))

    fun hasSubstring(property: String, value: String?, ignoreCase: Boolean): GremlinLeaf =
        GremlinLeaf(HasSubstring(property, value, !ignoreCase))

    fun hasPrefix(property: String, value: String): GremlinLeaf =
        GremlinLeaf(HasPrefix(property, value, false))

    fun hasElement(property: String, value: Any): GremlinLeaf =
        GremlinLeaf(HasElement(property, value))

    fun hasLinkTo(linkName: String, entity: Entity?) =
        if (entity == null) hasNoLink(linkName)
        else GremlinLeaf(HasLinkTo(linkName, (entity.id as YTDBEntityId).asOId()))

    fun hasNoLink(linkName: String) = GremlinLeaf(HasNoLink(linkName))
    fun hasLink(linkName: String) = GremlinLeaf(HasLink(linkName))

    fun inRange(property: String, min: Comparable<*>, max: Comparable<*>) =
        GremlinLeaf(PropInRange(property, min, max))

    fun or(left: NodeBase, right: NodeBase): GremlinBinaryNode =
        GremlinBinaryNode(
            left, right, true, "or",
            // todo: is it valid to use "union", which returns Set here?
            ::Or, Iterable<Entity>::union
        )

    fun and(left: NodeBase, right: NodeBase): GremlinBinaryNode =
        GremlinBinaryNode(
            left, right, true, "and",
            ::And, ::intersectTwoIts
        )

    fun combine(first: NodeBase, second: NodeBase) =
        GremlinBinaryNode(
            first, second, commutative = false, "andThen",
            ::AndThen
        )

    fun not(nodeBase: NodeBase): GremlinUnaryNode =
        GremlinUnaryNode(nodeBase, "not", ::Not)

    fun sortBy(propName: String, direction: SortDirection) =
        GremlinLeaf(Sort(GremlinBlock.Sort.ByProp(propName), direction))

    fun sortByLinked(linkName: String, propName: String, direction: SortDirection) =
        GremlinLeaf(Sort(GremlinBlock.Sort.ByLinked(linkName, propName), direction))

    private fun intersectTwoIts(it1: Iterable<Entity>, it2: Iterable<Entity>): Iterable<Entity> {
        val s1 = StreamSupport.stream(it1.spliterator(), false)
        val s2 = StreamSupport.stream(it2.spliterator(), false).collect(Collectors.toSet())
        return s1.filter(s2::contains).collect(Collectors.toList())
    }
}
