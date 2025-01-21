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
package jetbrains.exodus.entitystore.youtrackdb.testutil

import jetbrains.exodus.entitystore.youtrackdb.YTDBStoreTransactionImpl
import jetbrains.exodus.entitystore.youtrackdb.YTDBVertexEntity
import jetbrains.exodus.entitystore.youtrackdb.getOrCreateVertexClass

class OUsersWithInheritanceTestCase(youTrackDB: InMemoryYouTrackDB) {

    val user1: YTDBVertexEntity
    val user2: YTDBVertexEntity


    val agent1: YTDBVertexEntity
    val agent2: YTDBVertexEntity

    val guest: YTDBVertexEntity

    val admin1: YTDBVertexEntity
    val admin2: YTDBVertexEntity


    init {

        youTrackDB.withSession { session ->
            val baseClass = session.getOrCreateVertexClass(BaseUser.CLASS)
            val subclasses = listOf(
                session.getOrCreateVertexClass(Guest.CLASS),
                session.getOrCreateVertexClass(User.CLASS),
                session.getOrCreateVertexClass(Admin.CLASS),
                session.getOrCreateVertexClass(Agent.CLASS),
            )
            subclasses.forEach {
                it.addSuperClass(session, baseClass)
            }
        }

        val tx = youTrackDB.store.beginTransaction() as YTDBStoreTransactionImpl
        user1 = tx.createUser(User.CLASS, "u1")
        user2 = tx.createUser(User.CLASS, "u2")

        agent1 = tx.createUser(Agent.CLASS, "ag1")
        agent2 = tx.createUser(Agent.CLASS, "ag2")

        admin1 = tx.createUser(Admin.CLASS, "ad1")
        admin2 = tx.createUser(Admin.CLASS, "ad2")

        guest = tx.createUser(Guest.CLASS, "g1")
        tx.commit()
    }
}
