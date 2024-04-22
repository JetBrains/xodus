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
package jetbrains.exodus.entitystore.orientdb

import com.orientechnologies.orient.core.db.ODatabaseSession
import com.orientechnologies.orient.core.record.OVertex
import jetbrains.exodus.entitystore.orientdb.testutil.InMemoryOrientDB
import jetbrains.exodus.entitystore.orientdb.testutil.Issues.CLASS
import jetbrains.exodus.entitystore.orientdb.testutil.OTestMixin
import jetbrains.exodus.entitystore.orientdb.testutil.createIssue
import org.junit.Assert
import org.junit.Rule
import org.junit.Test

class OPersistentStoreTest: OTestMixin {

    @Rule
    @JvmField
    val orientDbRule = InMemoryOrientDB()

    override val orientDb = orientDbRule

    @Test
    fun renameClassTest() {
        val summary = "Hello, your product does not work"
        orientDb.createIssue(summary)
        val store = orientDb.store

        val newClassName = "Other${CLASS}"
        store.renameEntityType(CLASS, newClassName)
        val issueByNewName = store.computeInExclusiveTransaction {
            it as OStoreTransaction
            (it.activeSession as ODatabaseSession).queryEntities("select from $newClassName", store).firstOrNull()
        }
        Assert.assertNotNull(issueByNewName)
        issueByNewName!!
        Assert.assertEquals(summary, issueByNewName.getProperty("name"))
    }

    @Test
    fun transactionPropertiesTest() {
        val issue = orientDb.createIssue("Hello, nothing works")
        val store = orientDb.store
        store.computeInTransaction {
            Assert.assertTrue(it.isIdempotent)
            issue.asVertex.reload<OVertex>()
            issue.setProperty("version", "22")
            Assert.assertFalse(it.isIdempotent)
        }
    }

    @Test
    fun `create and increment sequence`() {
        val store = orientDb.store
        val sequence = store.computeInTransaction {
            it.getSequence("first")
        }
        store.executeInTransaction {
            Assert.assertEquals(1, sequence.increment())
        }
        store.executeInTransaction {
            Assert.assertEquals(1,it.getSequence("first").get())
        }
    }

    @Test
    fun `create sequence with starting from`() {
        val store = orientDb.store
        val sequence = store.computeInTransaction {
            it.getSequence("first", 99)
        }
        store.executeInTransaction {
            Assert.assertEquals(100, sequence.increment())
        }
    }

    @Test
    fun `can set actual value to sequence`(){
        val store = orientDb.store
        val sequence = store.computeInTransaction {
            it.getSequence("first", 99)
        }
        store.executeInTransaction {
            sequence.set(400)
        }
        store.executeInTransaction {
            Assert.assertEquals(401, sequence.increment())
        }
    }
}
