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
package jetbrains.exodus;

import org.jetbrains.annotations.NotNull;
import org.junit.Assert;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

public class TestUtil {

    private static final int TEMP_DIR_ATTEMPTS = 10000;

    private TestUtil() {
    }

    public static void runWithExpectedException(@NotNull final Runnable runnable,
                                                @NotNull final Class<? extends Throwable> exceptionClass) {
        try {
            runnable.run();
        } catch (Throwable t) {
            if (exceptionClass.getName().equals(t.getClass().getName())) {
                return;
            }
        }
        Assert.assertTrue("Expected exception wasn't thrown", false);
    }

    public static long time(@NotNull final String text, @NotNull final Runnable runnable) {
        final long started = System.currentTimeMillis();
        runnable.run();
        final long finished = System.currentTimeMillis();
        final long time = finished - started;
        System.out.println(text + ", time elapsed: " + ((double) time / 1000.0f) + 's');
        return time;
    }

    // from Guava code
    public static File createTempDir() {
        File baseDir = new File(System.getProperty("java.io.tmpdir"));
        String baseName = System.currentTimeMillis() + "-";

        for (int counter = 0; counter < TEMP_DIR_ATTEMPTS; counter++) {
            File tempDir = new File(baseDir, baseName + counter);
            if (tempDir.mkdir()) {
                return tempDir;
            }
        }
        throw new IllegalStateException("Failed to create directory within "
                + TEMP_DIR_ATTEMPTS + " attempts (tried "
                + baseName + "0 to " + baseName + (TEMP_DIR_ATTEMPTS - 1) + ')');
    }

    public static boolean streamsEqual(InputStream s1, InputStream s2) throws IOException {
        return streamsEqual(s1, s2, true);
    }

    public static boolean streamsEqual(InputStream s1, InputStream s2, boolean closeStreams) throws IOException {
        try {
            while (true) {
                final int b1 = s1.read();
                final int b2 = s2.read();
                if (b1 != b2) {
                    return false;
                }
                if (b1 == -1) {
                    break;
                }
            }
            return true;
        } finally {
            if (closeStreams) {
                s1.close();
                s2.close();
            }
        }
    }
}
