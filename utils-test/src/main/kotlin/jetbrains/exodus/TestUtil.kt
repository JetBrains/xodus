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
package jetbrains.exodus

import org.junit.Assert
import java.io.File
import java.io.IOException
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Path

object TestUtil {
    @JvmStatic
    fun runWithExpectedException(
        runnable: Runnable,
        exceptionClass: Class<out Throwable?>
    ) {
        try {
            runnable.run()
        } catch (t: Throwable) {
            if (exceptionClass.name == t.javaClass.name) {
                return
            }
        }
        Assert.fail("Expected exception wasn't thrown")
    }

    @JvmStatic
    fun time(text: String, runnable: Runnable): Long {
        val started = System.currentTimeMillis()
        runnable.run()
        val finished = System.currentTimeMillis()
        val time = finished - started
        println(text + ", time elapsed: " + time.toDouble() / 1000.0f + 's')
        return time
    }

    // from Guava code
    @JvmStatic
    fun createTempDir(): File {
        val buildDir = System.getProperty("exodus.tests.buildDirectory")
        return try {
            if (buildDir != null) {
                return Files.createTempDirectory(Path.of(buildDir), "xodus-test").toFile()
            }
            println("Build directory is not set !!!")
            Files.createTempDirectory("xodus-test").toFile()
        } catch (e: IOException) {
            throw RuntimeException(e)
        }
    }

    @JvmOverloads
    @Throws(IOException::class)
    fun streamsEqual(s1: InputStream, s2: InputStream, closeStreams: Boolean = true): Boolean {
        return try {
            while (true) {
                val b1 = s1.read()
                val b2 = s2.read()
                if (b1 != b2) {
                    return false
                }
                if (b1 == -1) {
                    break
                }
            }
            true
        } finally {
            if (closeStreams) {
                s1.close()
                s2.close()
            }
        }
    }
}
