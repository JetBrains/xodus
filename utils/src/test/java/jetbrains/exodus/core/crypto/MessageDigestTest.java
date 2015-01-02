/**
 * Copyright 2010 - 2015 JetBrains s.r.o.
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
import org.junit.Assert;
import org.junit.Test;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MessageDigestTest {

    @Test
    public void testSHA256() throws NoSuchAlgorithmException {
        /**
         * samples taken from http://en.wikipedia.org/wiki/SHA_hash_functions
         */
        final MessageDigest md = MessageDigest.getInstance("SHA-256");
        md.update("The quick brown fox jumps over the lazy dog".getBytes());
        Assert.assertEquals("d7a8fbb307d7809469ca9abcb0082e4f8d5651e46d3cdb762d02d0bf37c9e592",
                HexUtil.byteArrayToString(md.digest()));
        md.reset();
        md.update("The quick brown fox jumps over the lazy cog".getBytes());
        Assert.assertEquals("e4c4d8f3bf76b692de791a173e05321150f7a345b46484fe427f6acc7ecc81be",
                HexUtil.byteArrayToString(md.digest()));
        md.reset();
        Assert.assertEquals("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                HexUtil.byteArrayToString(md.digest()));
    }

    @Test
    public void testMDUtil() {
        /**
         * samples taken from http://en.wikipedia.org/wiki/SHA_hash_functions
         */
        Assert.assertEquals("d7a8fbb307d7809469ca9abcb0082e4f8d5651e46d3cdb762d02d0bf37c9e592",
                MessageDigestUtil.sha256("The quick brown fox jumps over the lazy dog"));
        Assert.assertEquals("e4c4d8f3bf76b692de791a173e05321150f7a345b46484fe427f6acc7ecc81be",
                MessageDigestUtil.sha256("The quick brown fox jumps over the lazy cog"));
        Assert.assertEquals("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                MessageDigestUtil.sha256(""));
        Assert.assertEquals("5feceb66ffc86f38d952786c6d696c79c2dbc239dd4e91b46729d73a27fb57e9",
                MessageDigestUtil.sha256("0"));
    }

    @Test
    public void testHexUtil() {
        final String s = "0123456789abcdeffedcba9876543210";
        Assert.assertEquals(s, HexUtil.byteArrayToString(HexUtil.stringToByteArray(s)));
    }
}
