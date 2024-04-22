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
package jetbrains.exodus.entitystore

import jetbrains.exodus.TestUtil
import mu.KotlinLogging
import org.junit.rules.ExternalResource
import java.io.File

private val log = KotlinLogging.logger {}

class XodusTestDB: ExternalResource() {

    private lateinit var databaseFolder: File

    lateinit var store: PersistentEntityStore
        private set

    override fun before() {
        databaseFolder = File(TestUtil.createTempDir().absolutePath)
        log.info("Temporary data folder is created: $databaseFolder")

        store = PersistentEntityStores.newInstance(databaseFolder)
    }

    override fun after() {
        try {
            store.close()
        } catch(e: Throwable) {
            log.error(e) { "Error on closing the store: ${e.message}" }
        }

        try {
            require(databaseFolder.deleteRecursively()) { "Failed to delete $databaseFolder recursively" }
            log.info { "Temporary data folder is deleted: $databaseFolder" }
        } catch (e: Throwable) {
            log.error(e) { "Error on deleting the temp database folder ${e.message}" }
        }
    }

    fun <R> withTx(block: (StoreTransaction) -> R): R {
        val tx = store.beginTransaction()
        try {
            val result = block(tx)
            tx.commit()
            return result
        } catch(e: Throwable) {
            if (!tx.isFinished) {
                tx.abort()
            }
            throw e
        }
    }
}