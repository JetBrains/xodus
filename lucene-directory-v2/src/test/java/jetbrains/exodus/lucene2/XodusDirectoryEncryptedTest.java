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
package jetbrains.exodus.lucene2;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import jetbrains.exodus.env.EnvironmentConfig;
import org.apache.lucene.tests.util.TestRuleLimitSysouts;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@ThreadLeakFilters(filters = XodusThreadFilter.class)
@TestRuleLimitSysouts.Limit(bytes = 150 * 1024)
public class XodusDirectoryEncryptedTest extends XodusDirectoryBaseTest {
    @Override
    protected EnvironmentConfig getEnvironmentConfig() {
        final EnvironmentConfig config = new EnvironmentConfig();
        config.setLogCachePageSize(1024);
        config.setCipherId("jetbrains.exodus.crypto.streamciphers.JBChaChaStreamCipherProvider");
        config.setCipherKey("000102030405060708090a0b0c0d0e0f000102030405060708090a0b0c0d0e0f");
        config.setPrintInitMessage(false);
        config.setCipherBasicIV(314159262718281828L);

        return config;
    }

    @Test
    public void testIvGeneration() throws IOException {
        final var fileDir = createTempDir("testIvGeneration");
        final var totalFiles = 10000;

        final var allIvs = new ArrayList<Long>();

        for (int numOfPages : List.of(1, 2, 5)) {

            final var ivs = new ArrayList<Long>();
            try (var dir = getDirectory(fileDir, null, ivs::add, null)) {
                for (int i = 0; i < totalFiles; i++) {
                    randomFileOfNPages(dir, "file_" + numOfPages + "_" + i, numOfPages);
                }
            }

            allIvs.addAll(ivs);

            assertEquals(totalFiles * numOfPages, ivs.size());
        }

        checkIVsUniqueness(allIvs);
    }

    @Test
    public void testIvRecycling() throws IOException {

        final var fileDir = createTempDir("testIvRecycling");

        final var ivs1 = new ArrayList<Long>();
        try (var dir = getDirectory(fileDir, null, ivs1::add, null)) {
            for (int i = 0; i <= 98; i++) {
                randomFileOfNPages(dir, "file_" + i, 1);
            }

            assertEquals(99, ivs1.size());
            checkIVsUniqueness(ivs1);

            for (int i = 40; i <= 90; i++) {
                dir.deleteFile("file_" + i);
            }
            // files 91 - 99 still exist
        }

        final var ivs2 = new ArrayList<Long>();
        try (var dir = getDirectory(fileDir, null, ivs2::add, null)) {
            randomFileOfNPages(dir, "file_99", 1);

            assertEquals(1, ivs2.size());
            assertTrue(ivs1.getLast() < ivs2.getFirst());

            for (int i = 91; i <= 99; i++) {
                dir.deleteFile("file_" + i);
            }
            // now file_39 has the maximal IV
        }
        final var prevMaxIv = ivs1.get(39);

        final var ivs3 = new ArrayList<Long>();
        try (var dir = getDirectory(fileDir, null, ivs3::add, null)) {
            for (int i = 100; i <= 109; i++) {
                randomFileOfNPages(dir, "file_" + i, 1);
            }

            assertEquals(10, ivs3.size());
            checkIVsUniqueness(ivs3);

            assertTrue(ivs3.getFirst() > prevMaxIv);
            assertTrue(ivs3.getFirst() - prevMaxIv <= IvGenerator.MAX_INCREMENT);
        }
    }


    private static void checkIVsUniqueness(ArrayList<Long> ivsList) {
        // IVs are sorted and unique
        assertEquals(ivsList.stream().distinct().sorted().toList(), ivsList);
    }
}
