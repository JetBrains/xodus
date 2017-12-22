/**
 * Copyright 2010 - 2017 JetBrains s.r.o.
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
        Assert.assertEquals(createEnvironment(dir, null, null, null),
                openEnvironment(dir, CHACHA_CIPHER_ID,
                        "000102030405060708090a0b0c0d0e0f000102030405060708090a0b0c0d0e0f", 0, expectedException))

    }

    @TestFor(issue = "XD-666")
    @Test
    fun testCreateCipheredOpenPlain() {
        val dir = TestUtil.createTempDir()
        Assert.assertEquals(createEnvironment(dir, CHACHA_CIPHER_ID,
                "000102030405060708090a0b0c0d0e0f000102030405060708090a0b0c0d0e0f", 0),
                openEnvironment(dir, null, null, null, expectedException))
    }

    @TestFor(issue = "XD-666")
    @Test
    fun testCreateCipheredOpenCipheredBadId() {
        val dir = TestUtil.createTempDir()
        Assert.assertEquals(createEnvironment(dir, CHACHA_CIPHER_ID,
                "000102030405060708090a0b0c0d0e0f000102030405060708090a0b0c0d0e0f", 0),
                openEnvironment(dir, SALSA20_CIPHER_ID,
                        "000102030405060708090a0b0c0d0e0f000102030405060708090a0b0c0d0e0f", 0, expectedException))
    }

    @TestFor(issue = "XD-666")
    @Test
    fun testCreateCipheredOpenCipheredBadKey() {
        val dir = TestUtil.createTempDir()
        Assert.assertEquals(createEnvironment(dir, CHACHA_CIPHER_ID,
                "000102030405060708090a0b0c0d0e0f000102030405060708090a0b0c0d0e0f", 0),
                openEnvironment(dir, CHACHA_CIPHER_ID,
                        "010102030405060708090a0b0c0d0e0f000102030405060708090a0b0c0d0e0f", 0, expectedException))
    }

    @TestFor(issue = "XD-666")
    @Test
    fun testCreateCipheredOpenCipheredBadBasicIV() {
        val dir = TestUtil.createTempDir()
        Assert.assertEquals(createEnvironment(dir, CHACHA_CIPHER_ID,
                "000102030405060708090a0b0c0d0e0f000102030405060708090a0b0c0d0e0f", 0),
                openEnvironment(dir, CHACHA_CIPHER_ID,
                        "010102030405060708090a0b0c0d0e0f000102030405060708090a0b0c0d0e0f", 1, expectedException))
    }
}

// create environment with specified cipher parameters
private fun createEnvironment(dir: File, cipherId: String?, cipherKey: String?, basicIV: Long?): Long {
    val ec = newEnvironmentConfig(cipherId, cipherKey, basicIV)
    ec.isGcEnabled = false
    Environments.newInstance(dir, ec).use { env ->
        val store = env.computeInTransaction { txn -> env.openStore("NaturalInteger", StoreConfig.WITHOUT_DUPLICATES, txn) }
        // 10000 database roots
        for (i in 1..1000) {
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
            return (env as EnvironmentImpl).log.highAddress
        }
    } else {
        TestUtil.runWithExpectedException({
            Environments.newInstance(dir, ec)
        }, exceptionClass)
        var result = 0L
        dir.listFiles { _, name -> name.endsWith(LogUtil.LOG_FILE_EXTENSION) }.forEach { result += it.length() }
        return result
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
