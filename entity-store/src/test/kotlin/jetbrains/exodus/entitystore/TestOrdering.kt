/**
 * Copyright 2010 - 2023 JetBrains s.r.o.
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

import jetbrains.exodus.entitystore.tables.PropertyKey
import org.junit.Assert
import java.util.*

class TestOrdering : EntityStoreTestBase() {

    fun testDirectOrder() {
        val txn = storeTransaction
        for (i in 0..199) {
            val issue = txn.newEntity("Issue")
            issue.setProperty("whatever", true)
        }
        txn.flush()
        for ((i, e) in txn.getAll("Issue").withIndex()) {
            val id = e.id
            assertEquals(0, id.typeId)
            assertEquals(i.toLong(), id.localId)
        }
    }

    fun testBindings() {
        val entry0 = PropertyKey.propertyKeyToEntry(PropertyKey(15, 71))
        val entry1 = PropertyKey.propertyKeyToEntry(PropertyKey(16, 24))
        val entry2 = PropertyKey.propertyKeyToEntry(PropertyKey(128, 24))
        val entry3 = PropertyKey.propertyKeyToEntry(PropertyKey(245, 71))

        Assert.assertTrue(Arrays.compareUnsigned(entry0.bytesUnsafe, 0, entry0.length,
                entry1.bytesUnsafe, 0, entry1.length) < 0)
        Assert.assertTrue(Arrays.compareUnsigned(entry0.bytesUnsafe, 0, entry0.length,
                entry2.bytesUnsafe, 0, entry2.length) < 0)
        Assert.assertTrue(Arrays.compareUnsigned(entry0.bytesUnsafe, 0, entry0.length,
                entry3.bytesUnsafe, 0, entry3.length) < 0)

        Assert.assertTrue(Arrays.compareUnsigned(entry1.bytesUnsafe, 0, entry1.length,
                entry2.bytesUnsafe, 0, entry2.length) < 0)
        Assert.assertTrue(Arrays.compareUnsigned(entry1.bytesUnsafe, 0, entry1.length,
                entry3.bytesUnsafe, 0, entry3.length) < 0)
        Assert.assertTrue(Arrays.compareUnsigned(entry1.bytesUnsafe, 0, entry1.length,
                entry0.bytesUnsafe, 0, entry0.length) > 0)

        Assert.assertTrue(Arrays.compareUnsigned(entry2.bytesUnsafe, 0, entry2.length,
                entry3.bytesUnsafe, 0, entry3.length) < 0)
        Assert.assertTrue(Arrays.compareUnsigned(entry2.bytesUnsafe, 0, entry2.length,
                entry1.bytesUnsafe, 0, entry1.length) > 0)
        Assert.assertTrue(Arrays.compareUnsigned(entry2.bytesUnsafe, 0, entry2.length,
                entry0.bytesUnsafe, 0, entry0.length) > 0)
    }
}