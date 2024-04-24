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
import jetbrains.exodus.entitystore.orientdb.withSession
import jetbrains.exodus.kotlin.synchronized

class OModelMetaData(
    private val databaseProvider: ODatabaseProvider
) : ModelMetaDataImpl() {

    /*
    * addAssociation() is called before and after prepare().
    *
    * Before prepare() is called, it does not make sense to initialize associations in the database
    * because the classes may be not created yet. All associations added before prepare() get
    * created in onPrepared().
    *
    * After prepare() is called, that is during the application lifetime, we translate add/remove
    * association to the database.
    * */
    private var prepared = false

    override fun onPrepared(entitiesMetaData: MutableCollection<EntityMetaData>) {
        databaseProvider.withSession { session ->
            session.applySchema(entitiesMetaData, indexForEverySimpleProperty = true, applyLinkCardinality = true)
        }
        prepared = true
    }

    override fun onReset() {
        prepared = false
    }

    /*
    * The parent class uses a concurrent hash map for association metadata.
    * It kind of hints us that concurrent access is expected/possible.
    * So, we synchronize adding/removing associations here.
    * */

    override fun onAddAssociation(typeName: String, association: AssociationEndMetaData) {
        if (prepared) {
            synchronized {
                databaseProvider.withSession { session ->
                    session.addAssociation(typeName, association)
                }
            }
        }
    }

    override fun onRemoveAssociation(sourceTypeName: String, targetTypeName: String, associationName: String) {
        if (prepared) {
            synchronized {
                databaseProvider.withSession { session ->
                    session.removeAssociation(sourceTypeName, targetTypeName, associationName)
                }
            }
        }
    }
}