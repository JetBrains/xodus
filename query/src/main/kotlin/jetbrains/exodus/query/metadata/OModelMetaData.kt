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

import jetbrains.exodus.entitystore.orientdb.ODatabaseProvider
import jetbrains.exodus.entitystore.orientdb.withCurrentOrNewSession
import jetbrains.exodus.kotlin.synchronized

class OModelMetaData(
    private val databaseProvider: ODatabaseProvider
) : ModelMetaDataImpl() {


    override fun onPrepared(entitiesMetaData: MutableCollection<EntityMetaData>) {
        databaseProvider.withCurrentOrNewSession(requireNoActiveTransaction = true) { session ->
            val indices = session.applySchema(entitiesMetaData, indexForEverySimpleProperty = true, applyLinkCardinality = true)
            session.applyIndices(indices)
        }
    }

    /*
    * The parent class uses a concurrent hash map for association metadata.
    * It kind of hints us that concurrent access is expected/possible.
    * So, we synchronize adding/removing associations here.
    * */

    override fun onAddAssociation(typeName: String, association: AssociationEndMetaData) {
        synchronized {
            databaseProvider.withCurrentOrNewSession(requireNoActiveTransaction = true) { session ->
                session.addAssociation(typeName, association)
            }
        }
    }

    override fun onRemoveAssociation(sourceTypeName: String, targetTypeName: String, associationName: String) {
        synchronized {
            databaseProvider.withCurrentOrNewSession(requireNoActiveTransaction = true) { session ->
                session.removeAssociation(sourceTypeName, targetTypeName, associationName)
            }
        }
    }
}