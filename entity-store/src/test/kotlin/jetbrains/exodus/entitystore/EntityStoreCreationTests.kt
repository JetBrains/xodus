/**
 * Copyright 2010 - 2020 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.entitystore

import jetbrains.exodus.TestFor
import jetbrains.exodus.env.Environments
import org.junit.Assert
import org.junit.Test

class EntityStoreCreationTests : EntityStoreTestBase() {

    @TestFor(issue = "XD-787")
    @Test
    fun testXD_787() {
        initTempFolder().let { tempFolder ->
            try {
                Environments.newInstance(tempFolder).use { env ->
                    PersistentEntityStores.newInstance(env, "S1").use {
                        PersistentEntityStores.newInstance(env, "S2").use { }
                        Assert.assertTrue(env.isOpen)
                    }
                    Assert.assertTrue(env.isOpen)
                }
            } finally {
                cleanUp(tempFolder)
            }
        }
    }

    @TestFor(issue = "XD-787")
    @Test
    fun testXD_787_extra() {
        initTempFolder().let { tempFolder ->
            try {
                val store = PersistentEntityStores.newInstance(tempFolder)
                store.use {}
                Assert.assertFalse(store.environment.isOpen)
            } finally {
                cleanUp(tempFolder)
            }
        }
    }
}