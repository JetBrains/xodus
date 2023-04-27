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
package jetbrains.exodus.util

import jetbrains.exodus.util.CompressBackupUtil.tar
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.junit.After
import org.junit.Assert
import org.junit.Before
import org.junit.Test
import java.io.BufferedInputStream
import java.io.File
import java.io.FileInputStream
import java.io.FileWriter
import java.util.zip.GZIPInputStream

class CompressBackupUtilTest {
    private var randName: String? = null
    private var dest: File? = null
    @Before
    fun setUp() {
        randName = javaClass.classLoader.getResource(".")!!.file + RANDOM.nextLong()
        dest = File("$randName.tar.gz")
    }

    @After
    fun tearDown() {
        if (!dest!!.delete()) {
            dest!!.deleteOnExit()
            //            Assert.fail("Can't delete file after test.");
        }
    }

    @Test
    @Throws(Exception::class)
    fun testNoSource() {
        try {
            tar(File("$randName.txt"), dest!!)
        } catch (e: IllegalArgumentException) {
            return
        }
        Assert.fail("No source file/folder exists. Should have thrown an exception.")
    }

    @Test
    @Throws(Exception::class)
    fun testDestExists() {
        dest!!.createNewFile()
        try {
            tar(File("$randName.txt"), dest!!)
        } catch (e: IllegalArgumentException) {
            return
        }
        Assert.fail("Destination file/folder already exists. Should have thrown an exception.")
    }

    @Test
    @Throws(Exception::class)
    fun testFileArchived() {
        val src = File("$randName.txt")
        val fw = FileWriter(src)
        fw.write("12345")
        fw.close()
        tar(src, dest!!)
        Assert.assertTrue("No destination archive created", dest!!.exists())
        val tai = TarArchiveInputStream(GZIPInputStream(BufferedInputStream(FileInputStream(dest!!))))
        val entry = tai.nextEntry
        Assert.assertNotNull("No entry found in destination archive", entry)
        Assert.assertEquals("Entry has wrong size", 5, entry.size)
    }

    @Test
    @Throws(Exception::class)
    fun testFolderArchived() {
        val src = File(randName!!)
        src.mkdir()
        var fw = FileWriter(File(src, "1.txt"))
        fw.write("12345")
        fw.close()
        fw = FileWriter(File(src, "2.txt"))
        fw.write("12")
        fw.close()
        tar(src, dest!!)
        Assert.assertTrue("No destination archive created", dest!!.exists())
        val tai = TarArchiveInputStream(GZIPInputStream(BufferedInputStream(FileInputStream(dest!!))))
        var entry1 = tai.nextEntry
        var entry2 = tai.nextEntry
        if (entry1.name > entry2.name) { // kinda sort them lol
            val tmp = entry1
            entry1 = entry2
            entry2 = tmp
        }
        Assert.assertNotNull("No entry found in destination archive", entry1)
        Assert.assertEquals("Entry has wrong size", 5, entry1.size)
        println(entry1.name)
        Assert.assertEquals("Entry has wrong relative path", src.name + "/1.txt", entry1.name)
        println(entry2.name)
        Assert.assertEquals("Entry has wrong size", 2, entry2.size)
        Assert.assertEquals("Entry has wrong relative path", src.name + "/2.txt", entry2.name)
    }

    companion object {
        private val RANDOM = Random(System.currentTimeMillis())
    }
}
