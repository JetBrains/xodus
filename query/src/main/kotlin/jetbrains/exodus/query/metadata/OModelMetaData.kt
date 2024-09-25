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

import com.orientechnologies.orient.core.db.ODatabaseSession
import com.orientechnologies.orient.core.metadata.schema.OClass
import com.orientechnologies.orient.core.record.ODirection
import com.orientechnologies.orient.core.record.OVertex
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
        session: ODatabaseSession,
        linkName: String,
        outClassName: String,
        inClassName: String
    ): OClass {
        /**
         * It is not enough to check the existence of the edge class.
         * We reuse the same edge class for all the links with the same name.
         * So, we have to check the existence of the edge class + in and out properties of the connected classes.
         */
        // edge class
        val edgeClassName = OVertexEntity.edgeClassName(linkName)
        val oClass = session.getClass(edgeClassName)
        // out-property
        val outClass = session.getClass(outClassName) ?: throw IllegalStateException("$outClassName not found. It must not ever happen.")
        val outPropName = OVertex.getEdgeLinkFieldName(ODirection.OUT, edgeClassName)
        val outProp = outClass.getProperty(outPropName)
        // in-property
        val inClass = session.getClass(inClassName) ?: throw IllegalStateException("$inClassName not found. It must not ever happen.")
        val inPropName = OVertex.getEdgeLinkFieldName(ODirection.IN, edgeClassName)
        val inProp = inClass.getProperty(inPropName)
        if (oClass != null && outProp != null && inProp != null) {
            return oClass
        }

        return session.executeInASeparateSessionIfCurrentHasTransaction(dbProvider) { sessionToWork ->
            val link = LinkMetadata(name = linkName, outClassName = outClassName, inClassName = inClassName, AssociationEndCardinality._0_n)
            val result = sessionToWork.addAssociation(link, indicesContainingLink = listOf(), applyLinkCardinality = true)
            sessionToWork.initializeIndices(result)
            sessionToWork.getClass(edgeClassName) ?: throw IllegalStateException("$edgeClassName not found, it must never happen")
        }
    }
}