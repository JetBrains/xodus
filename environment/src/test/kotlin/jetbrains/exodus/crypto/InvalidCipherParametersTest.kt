/**
 * Copyright 2010 - 2018 JetBrains s.r.o.
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
package jetbrains.exodus.crypto

import jetbrains.exodus.TestFor
import jetbrains.exodus.TestUtil
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.bindings.StringBinding
import jetbrains.exodus.crypto.streamciphers.CHACHA_CIPHER_ID
import jetbrains.exodus.crypto.streamciphers.SALSA20_CIPHER_ID
import jetbrains.exodus.env.EnvironmentConfig
import jetbrains.exodus.env.EnvironmentImpl
import jetbrains.exodus.env.Environments
import jetbrains.exodus.env.StoreConfig
import jetbrains.exodus.log.LogUtil
import org.junit.Assert
import org.junit.Test
import java.io.File

private const val ENTRIES = 1000
private val expectedException = InvalidCipherParametersException::class.java

class InvalidCipherParametersTest {

    @TestFor(issue = "XD-666")
    @Test
    fun testCreatePlainOpenPlain() {
        val dir = TestUtil.createTempDir()
        Assert.assertEquals(createEnvironment(dir, null, null, null),
                openEnvironment(dir, null, null, null))
    }

    @TestFor(issue = "XD-666")
    @Test
    fun testCreatePlainOpenCiphered() {
        val dir = TestUtil.createTempDir()
        val highAddress = createEnvironment(dir, null, null, null)
        Assert.assertEquals(highAddress,
                openEnvironment(dir, CHACHA_CIPHER_ID,
                        "000102030405060708090a0b0c0d0e0f000102030405060708090a0b0c0d0e0f", 0, expectedException))
        Assert.assertEquals(highAddress, openEnvironment(dir, null, null, null))
    }

    @TestFor(issue = "XD-666")
    @Test
    fun testCreateCipheredOpenPlain() {
        val dir = TestUtil.createTempDir()
        val highAddress = createEnvironment(dir, CHACHA_CIPHER_ID,
                "000102030405060708090a0b0c0d0e0f000102030405060708090a0b0c0d0e0f", 0)
        Assert.assertEquals(highAddress, openEnvironment(dir, null, null, null, expectedException))
        Assert.assertEquals(highAddress, openEnvironment(dir, CHACHA_CIPHER_ID,
                "000102030405060708090a0b0c0d0e0f000102030405060708090a0b0c0d0e0f", 0))
    }

    @TestFor(issue = "XD-666")
    @Test
    fun testCreateCipheredOpenCipheredBadId() {
        val dir = TestUtil.createTempDir()
        val highAddress = createEnvironment(dir, CHACHA_CIPHER_ID,
                "000102030405060708090a0b0c0d0e0f000102030405060708090a0b0c0d0e0f", 0)
        Assert.assertEquals(highAddress, openEnvironment(dir, SALSA20_CIPHER_ID,
                "000102030405060708090a0b0c0d0e0f000102030405060708090a0b0c0d0e0f", 0, expectedException))
        Assert.assertEquals(highAddress, openEnvironment(dir, CHACHA_CIPHER_ID,
                "000102030405060708090a0b0c0d0e0f000102030405060708090a0b0c0d0e0f", 0))
    }

    @TestFor(issue = "XD-666")
    @Test
    fun testCreateCipheredOpenCipheredBadKey() {
        val dir = TestUtil.createTempDir()
        val highAddress = createEnvironment(dir, CHACHA_CIPHER_ID,
                "000102030405060708090a0b0c0d0e0f000102030405060708090a0b0c0d0e0f", 0)
        Assert.assertEquals(highAddress, openEnvironment(dir, CHACHA_CIPHER_ID,
                "010102030405060708090a0b0c0d0e0f000102030405060708090a0b0c0d0e0f", 0, expectedException))
        Assert.assertEquals(highAddress, openEnvironment(dir, CHACHA_CIPHER_ID,
                "000102030405060708090a0b0c0d0e0f000102030405060708090a0b0c0d0e0f", 0))
    }

    @TestFor(issue = "XD-666")
    @Test
    fun testCreateCipheredOpenCipheredBadBasicIV() {
        val dir = TestUtil.createTempDir()
        val highAddress = createEnvironment(dir, CHACHA_CIPHER_ID,
                "000102030405060708090a0b0c0d0e0f000102030405060708090a0b0c0d0e0f", 0)
        Assert.assertEquals(highAddress, openEnvironment(dir, CHACHA_CIPHER_ID,
                "010102030405060708090a0b0c0d0e0f000102030405060708090a0b0c0d0e0f", 1, expectedException))
        Assert.assertEquals(highAddress, openEnvironment(dir, CHACHA_CIPHER_ID,
                "000102030405060708090a0b0c0d0e0f000102030405060708090a0b0c0d0e0f", 0))
    }
}

// create environment with specified cipher parameters
private fun createEnvironment(dir: File, cipherId: String?, cipherKey: String?, basicIV: Long?): Long {
    val ec = newEnvironmentConfig(cipherId, cipherKey, basicIV)
    ec.gcStartIn = 10
    ec.gcFilesDeletionDelay = 0
    Environments.newInstance(dir, ec).use { env ->
        val store = env.computeInTransaction { txn ->
            env.openStore(
                    "NaturalInteger", StoreConfig.WITHOUT_DUPLICATES, txn)
        }
        for (i in 0 until ENTRIES) {
            env.executeInTransaction({ txn ->
                store.put(txn, IntegerBinding.intToCompressedEntry(i), StringBinding.stringToEntry(i.toString(10)))
            })
        }
        ec.envIsReadonly = true
        return (env as EnvironmentImpl).log.highAddress
    }
}

// open environment with specified cipher parameters
private fun openEnvironment(dir: File, cipherId: String?, cipherKey: String?, basicIV: Long?,
                            exceptionClass: Class<out Throwable>? = null): Long {
    val ec = newEnvironmentConfig(cipherId, cipherKey, basicIV)
    if (exceptionClass == null) {
        Environments.newInstance(dir, ec).use { env ->
            val store = env.computeInTransaction { txn ->
                env.openStore(
                        "NaturalInteger", StoreConfig.USE_EXISTING, txn)
            }
            env.executeInReadonlyTransaction { txn ->
                for (i in 0 until ENTRIES) {
                    Assert.assertEquals(StringBinding.stringToEntry(i.toString(10)),
                            store.get(txn, IntegerBinding.intToCompressedEntry(i)))
                }
                Assert.assertNull(store.get(txn, IntegerBinding.intToCompressedEntry(ENTRIES)))
            }
            return (env as EnvironmentImpl).log.highAddress
        }
    } else {
        TestUtil.runWithExpectedException({
            Environments.newInstance(dir, ec)
        }, exceptionClass)
        val highFile = dir.list { _, name -> name.endsWith(LogUtil.LOG_FILE_EXTENSION) }.apply { sortDescending() }[0]
        return LogUtil.getAddress(highFile) + File(dir, highFile).length()
    }
}

private fun newEnvironmentConfig(cipherId: String?, cipherKey: String?, basicIV: Long?): EnvironmentConfig {
    val ec = EnvironmentConfig()
    if (cipherId == null) {
        ec.removeSetting(EnvironmentConfig.CIPHER_ID)
    } else {
        ec.cipherId = cipherId
        ec.setCipherKey(cipherKey)
        basicIV?.let { ec.cipherBasicIV = it }
    }

    ec.logCachePageSize = 4096
    ec.logFileSize = 4L
    return ec
}
