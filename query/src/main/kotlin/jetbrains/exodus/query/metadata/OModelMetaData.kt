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

import com.jetbrains.youtrack.db.api.DatabaseSession
import com.jetbrains.youtrack.db.api.schema.SchemaClass
import jetbrains.exodus.entitystore.orientdb.*

class OModelMetaData(
    private val dbProvider: ODatabaseProvider,
    private val schemaBuddy: OSchemaBuddy = OSchemaBuddyImpl(dbProvider, autoInitialize = false)
) : ModelMetaDataImpl(), OSchemaBuddy by schemaBuddy {

    override fun onPrepared(entitiesMetaData: MutableCollection<EntityMetaData>) {
        dbProvider.withCurrentOrNewSession(requireNoActiveTransaction = true) { session ->
            val result = session.applySchema(entitiesMetaData, indexForEverySimpleProperty = true, applyLinkCardinality = true)
            session.initializeIndices(result)
            initialize(session)
        }
    }

    override fun onAddAssociation(entityMetaData: EntityMetaData, association: AssociationEndMetaData) {
        dbProvider.withCurrentOrNewSession(requireNoActiveTransaction = true) { session ->
            val result = session.addAssociation(entityMetaData, association)
            session.initializeIndices(result)
        }
    }

    override fun onRemoveAssociation(sourceTypeName: String, targetTypeName: String, associationName: String) {
        dbProvider.withCurrentOrNewSession(requireNoActiveTransaction = true) { session ->
            session.removeAssociation(sourceTypeName, targetTypeName, associationName)
        }
    }

    override fun getOrCreateEdgeClass(
        session: DatabaseSession,
        linkName: String,
        outClassName: String,
        inClassName: String
    ): SchemaClass {
        /**
         * It is enough to check the existence of the edge class.
         * We reuse the same edge class for all the links with the same name.
         */
        val edgeClassName = OVertexEntity.edgeClassName(linkName)
        val oClass = session.getClass(edgeClassName)
        if (oClass != null) {
            return oClass
        }

        return session.executeInASeparateSessionIfCurrentHasTransaction(dbProvider) { sessionToWork ->
            val link = LinkMetadata(name = linkName, outClassName = outClassName, inClassName = inClassName, AssociationEndCardinality._0_n)

            /**
             * We do not apply link cardinality because:
             * 1. We do not have any cardinality restrictions for ad-hoc links.
             * 2. Applying the cardinality causes adding extra properties to existing vertices,
             * that in turn potentially causes OConcurrentModificationException in the original session.
             * Keep in mind that we create this edge class (and those extra properties) in a separate session.
             * So, there is an original session with the business logic that may fail if we change vertices here.
             */
            val result = sessionToWork.addAssociation(link, indicesContainingLink = listOf(), applyLinkCardinality = false)
            sessionToWork.initializeIndices(result)
            sessionToWork.getClass(edgeClassName) ?: throw IllegalStateException("$edgeClassName not found, it must never happen")
        }
    }
}