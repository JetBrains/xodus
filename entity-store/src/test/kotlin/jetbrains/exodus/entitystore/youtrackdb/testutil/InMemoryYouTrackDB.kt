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

import YTDBDatabaseProviderFactory
import YouTrackDBFactory
import com.jetbrains.youtrack.db.api.DatabaseSession
import com.jetbrains.youtrack.db.api.DatabaseType
import com.jetbrains.youtrack.db.api.YouTrackDB
import jetbrains.exodus.entitystore.youtrackdb.*
import jetbrains.exodus.entitystore.youtrackdb.testutil.Issues.Links.IN_PROJECT
import jetbrains.exodus.entitystore.youtrackdb.testutil.Issues.Links.ON_BOARD
import jetbrains.exodus.entitystore.youtrackdb.testutil.Projects.Links.HAS_ISSUE
import org.junit.rules.ExternalResource
import java.nio.file.Files
import kotlin.io.path.absolutePathString

class InMemoryYouTrackDB(
    private val initializeIssueSchema: Boolean = true,
    private val autoInitializeSchemaBuddy: Boolean = true
) : ExternalResource() {

    private lateinit var db: YouTrackDB
    lateinit var store: YTDBPersistentEntityStore
        private set

    lateinit var provider: YTDBDatabaseProvider
    lateinit var schemaBuddy: YTDBSchemaBuddyImpl

    val username = "admin"
    val password = "password"
    val dbName = "testDB"

    override fun before() {
        val params = YTDBDatabaseParams.builder()
            .withDatabaseType(DatabaseType.MEMORY)
            .withDatabasePath(Files.createTempDirectory("youTrackDB_test").absolutePathString())
            .withPassword(password)
            .withUserName(username)
            .withDatabaseName(dbName)
            .build()

        db = YouTrackDBFactory.createEmbedded(params)
        provider = YTDBDatabaseProviderFactory.createProvider(params, db) as YTDBDatabaseProviderImpl

        if (initializeIssueSchema) {
            provider.withSession { session ->
                session.getOrCreateVertexClass(Issues.CLASS)
                session.getOrCreateVertexClass(Boards.CLASS)
                session.getOrCreateVertexClass(Projects.CLASS)
                session.addAssociation(Issues.CLASS, Boards.CLASS, ON_BOARD, HAS_ISSUE)
                session.addAssociation(Boards.CLASS, Issues.CLASS, HAS_ISSUE, ON_BOARD)
                session.addAssociation(Issues.CLASS, Projects.CLASS, IN_PROJECT, HAS_ISSUE)
                session.addAssociation(Projects.CLASS, Issues.CLASS, HAS_ISSUE, IN_PROJECT)
            }
        }

        schemaBuddy = YTDBSchemaBuddyImpl(provider, autoInitialize = autoInitializeSchemaBuddy)
        store = YTDBPersistentEntityStore(provider, dbName,
            schemaBuddy = schemaBuddy
        )
    }

    override fun after() {
        db.close()
    }

    val database get() = db

    fun <R> withStoreTx(block: (YTDBStoreTransaction) -> R): R {
        return store.computeInTransaction { tx ->
            block(tx as YTDBStoreTransaction)
        }
    }

    fun <R> withTxSession(block: (DatabaseSession) -> R): R {
        val session = provider.acquireSession()
        try {
            session.begin()
            val result = block(session)
            if (session.hasActiveTransaction()) {
                session.commit()
            }
            return result
        } finally {
            if (!session.hasActiveTransaction()) {
                session.rollback()
            }
            if (!session.isClosed) {
                session.close()
            }
        }
    }

    fun <R> withSession(block: (DatabaseSession) -> R): R {
        val session = provider.acquireSession()
        try {
            return block(session)
        } finally {
            if (!session.isClosed) {
                session.close()
            }
        }
    }

    fun openSession(): DatabaseSession {
        return db.cachedPool(dbName, username, password).acquire()
    }
}
