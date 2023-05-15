/*
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
package jetbrains.exodus.core.crypto

import jetbrains.exodus.util.HexUtil
import java.security.MessageDigest
import java.security.NoSuchAlgorithmException

object MessageDigestUtil {
    fun MD5(message: String): String {
        return encode(message, "MD5", null)
    }

    fun MD5(message: ByteArray): ByteArray? {
        return encode(message, "MD5")
    }

    fun sha1(message: String): String {
        return encode(message, "SHA-1", null)
    }

    fun sha1(message: ByteArray): ByteArray? {
        return encode(message, "SHA-1")
    }

    fun sha256(message: String): String {
        return encode(message, "SHA-256", "UTF-8")
    }

    fun sha256(message: ByteArray): ByteArray? {
        return encode(message, "SHA-256")
    }

    fun hmacSha1(key: ByteArray, message: String): String {
        return hmacShaX("HMACSHA1", key, message, "UTF-8")
    }

    fun hmacSha1(key: ByteArray, message: ByteArray): ByteArray {
        return hmacShaX("HMACSHA1", key, message)
    }

    fun hmacSha256(key: ByteArray, message: String): String {
        return hmacShaX("HMACSHA256", key, message, "UTF-8")
    }

    fun hmacSha256(key: ByteArray, message: ByteArray): ByteArray {
        return hmacShaX("HMACSHA256", key, message)
    }

    private fun encode(message: String, method: String, encoding: String?): String {
        return try {
            val encoded: ByteArray
            encoded = if (encoding != null) {
                message.toByteArray(charset(encoding))
            } else {
                message.toByteArray()
            }
            HexUtil.byteArrayToString(encodeUnsafe(encoded, method)!!)
        } catch (e: Exception) {
            message
        }
    }

    private fun encode(message: ByteArray, method: String): ByteArray? {
        return try {
            encodeUnsafe(message, method)
        } catch (e: Exception) {
            message
        }
    }

    @Throws(NoSuchAlgorithmException::class)
    private fun encodeUnsafe(message: ByteArray?, method: String): ByteArray? {
        if (message == null) return null
        val md = MessageDigest.getInstance(method)
        md.update(message)
        return md.digest()
    }

    private fun hmacShaX(method: String, key: ByteArray, message: String, encoding: String?): String {
        return try {
            val encoded: ByteArray
            encoded = if (encoding != null) {
                message.toByteArray(charset(encoding))
            } else {
                message.toByteArray()
            }
            HexUtil.byteArrayToString(hmacShaXUnsafe(method, key, encoded))
        } catch (e: Exception) {
            message
        }
    }

    private fun hmacShaX(method: String, key: ByteArray, message: ByteArray): ByteArray {
        return try {
            hmacShaXUnsafe(method, key, message)
        } catch (e: Exception) {
            message
        }
    }

    private fun hmacShaXUnsafe(method: String, key: ByteArray, message: ByteArray): ByteArray {
        /* if (message == null) return null;
      SecretKey sk = new SecretKeySpec(key, method);
      final Mac m = Mac.getInstance(sk.getAlgorithm());
      m.init(sk);
      return m.doFinal(message); */
        throw UnsupportedOperationException("Is not supported as there is no javax.crypto package in MPS JDK")
    }
}
