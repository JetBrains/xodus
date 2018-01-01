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
package jetbrains.exodus.core.crypto;

import jetbrains.exodus.util.HexUtil;
import org.jetbrains.annotations.Nullable;

import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

// import javax.crypto.SecretKey;
// import javax.crypto.Mac;
// import javax.crypto.spec.SecretKeySpec;

public class MessageDigestUtil {

    private MessageDigestUtil() {
    }

    public static String MD5(final String message) {
        return encode(message, "MD5", null);
    }

    public static byte[] MD5(final byte[] message) {
        return encode(message, "MD5");
    }

    public static String sha1(final String message) {
        return encode(message, "SHA-1", null);
    }

    public static byte[] sha1(final byte[] message) {
        return encode(message, "SHA-1");
    }

    public static String sha256(final String message) {
        return encode(message, "SHA-256", "UTF-8");
    }

    public static byte[] sha256(final byte[] message) {
        return encode(message, "SHA-256");
    }

    public static String hmacSha1(byte[] key, String message) {
        return hmacShaX("HMACSHA1", key, message, "UTF-8");
    }

    public static byte[] hmacSha1(byte[] key, byte[] message) {
        return hmacShaX("HMACSHA1", key, message);
    }

    public static String hmacSha256(byte[] key, String message) {
        return hmacShaX("HMACSHA256", key, message, "UTF-8");
    }

    public static byte[] hmacSha256(byte[] key, byte[] message) {
        return hmacShaX("HMACSHA256", key, message);
    }

    private static String encode(String message, String method, @Nullable String encoding) {
        try {
            byte[] encoded;
            if (encoding != null) {
                encoded = message.getBytes(encoding);
            } else {
                encoded = message.getBytes();
            }
            return HexUtil.byteArrayToString(encodeUnsafe(encoded, method));
        } catch (Exception e) {
            return message;
        }
    }

    private static byte[] encode(byte[] message, String method) {
        try {
            return encodeUnsafe(message, method);
        } catch (Exception e) {
            return message;
        }
    }

    private static byte[] encodeUnsafe(byte[] message, String method) throws NoSuchAlgorithmException {
        if (message == null) return null;
        final MessageDigest md = MessageDigest.getInstance(method);
        md.update(message);
        return md.digest();
    }

    private static String hmacShaX(String method, byte[] key, String message, String encoding) {
        try {
            byte[] encoded;
            if (encoding != null) {
                encoded = message.getBytes(encoding);
            } else {
                encoded = message.getBytes();
            }
            return HexUtil.byteArrayToString(hmacShaXUnsafe(method, key, encoded));
        } catch (Exception e) {
            return message;
        }
    }

    private static byte[] hmacShaX(String method, byte[] key, byte[] message) {
        try {
            return hmacShaXUnsafe(method, key, message);
        } catch (Exception e) {
            return message;
        }
    }

    private static byte[] hmacShaXUnsafe(String method, byte[] key, byte[] message) throws NoSuchAlgorithmException, InvalidKeyException {
        /* if (message == null) return null;
      SecretKey sk = new SecretKeySpec(key, method);
      final Mac m = Mac.getInstance(sk.getAlgorithm());
      m.init(sk);
      return m.doFinal(message); */
        throw new UnsupportedOperationException("Is not supported as there is no javax.crypto package in MPS JDK");
    }

}
