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
import jetbrains.exodus.entitystore.PersistentEntityId
import jetbrains.exodus.entitystore.orientdb.*

class OModelMetaData(
    private val databaseProvider: ODatabaseProvider,
    private val schemaBuddy: OSchemaBuddy = OSchemaBuddyImpl(databaseProvider, autoInitialize = false)
) : ModelMetaDataImpl(), OSchemaBuddy {

    override fun onPrepared(entitiesMetaData: MutableCollection<EntityMetaData>) {
        databaseProvider.withCurrentOrNewSession(requireNoActiveTransaction = true) { session ->
            val result = session.applySchema(entitiesMetaData, indexForEverySimpleProperty = true, applyLinkCardinality = true)
            session.initializeIndices(result)
            initialize(session)
        }
    }

    override fun onAddAssociation(entityMetaData: EntityMetaData, association: AssociationEndMetaData) {
        databaseProvider.withCurrentOrNewSession(requireNoActiveTransaction = true) { session ->
            val result = session.addAssociation(entityMetaData, association)
            session.initializeIndices(result)
        }
    }

    private fun ODatabaseSession.initializeIndices(schemaApplicationResult: SchemaApplicationResult) {
        /*
        * The order of operations matter.
        * We want to initialize complementary properties before creating indices,
        * it is more efficient from the performance point of view.
        * */
        initializeComplementaryPropertiesForNewIndexedLinks(schemaApplicationResult.newIndexedLinks)
        applyIndices(schemaApplicationResult.indices)
    }

    override fun onRemoveAssociation(sourceTypeName: String, targetTypeName: String, associationName: String) {
        databaseProvider.withCurrentOrNewSession(requireNoActiveTransaction = true) { session ->
            session.removeAssociation(sourceTypeName, targetTypeName, associationName)
        }
    }

    override fun getOEntityId(session: ODatabaseSession, entityId: PersistentEntityId): ORIDEntityId {
        return schemaBuddy.getOEntityId(session, entityId)
    }

    override fun makeSureTypeExists(session: ODatabaseSession, entityType: String) {
        schemaBuddy.makeSureTypeExists(session, entityType)
    }

    override fun initialize(session: ODatabaseSession) {
        schemaBuddy.initialize(session)
    }
}