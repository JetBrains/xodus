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
package jetbrains.exodus.util;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.util.zip.GZIPInputStream;

public class CompressBackupUtilTest {
    private static final Random RANDOM = new Random(System.currentTimeMillis());

    private String randName;
    private File dest;

    @Before
    public void setUp() throws Exception {
        randName = getClass().getClassLoader().getResource(".").getFile() + RANDOM.nextLong();
        dest = new File(randName + ".tar.gz");
    }

    @After
    public void tearDown() throws Exception {
        if (!dest.delete()) {
            dest.deleteOnExit();
//            Assert.fail("Can't delete file after test.");
        }
    }

    @Test
    public void testNoSource() throws Exception {
        try {
            CompressBackupUtil.tar(new File(randName + ".txt"), dest);
        } catch (IllegalArgumentException e) {
            return;
        }
        Assert.fail("No source file/folder exists. Should have thrown an exception.");
    }

    @Test
    public void testDestExists() throws Exception {
        dest.createNewFile();
        try {
            CompressBackupUtil.tar(new File(randName + ".txt"), dest);
        } catch (IllegalArgumentException e) {
            return;
        }
        Assert.fail("Destination file/folder already exists. Should have thrown an exception.");
    }

    @Test
    public void testFileArchived() throws Exception {
        File src = new File(randName + ".txt");
        FileWriter fw = new FileWriter(src);
        fw.write("12345");
        fw.close();
        CompressBackupUtil.tar(src, dest);
        Assert.assertTrue("No destination archive created", dest.exists());
        TarArchiveInputStream tai = new TarArchiveInputStream(new GZIPInputStream(new BufferedInputStream(new FileInputStream(dest))));
        ArchiveEntry entry = tai.getNextEntry();
        Assert.assertNotNull("No entry found in destination archive", entry);
        Assert.assertEquals("Entry has wrong size", 5, entry.getSize());
    }

    @Test
    public void testFolderArchived() throws Exception {
        File src = new File(randName);
        src.mkdir();
        FileWriter fw = new FileWriter(new File(src, "1.txt"));
        fw.write("12345");
        fw.close();
        fw = new FileWriter(new File(src, "2.txt"));
        fw.write("12");
        fw.close();
        CompressBackupUtil.tar(src, dest);
        Assert.assertTrue("No destination archive created", dest.exists());
        TarArchiveInputStream tai = new TarArchiveInputStream(new GZIPInputStream(new BufferedInputStream(new FileInputStream(dest))));
        ArchiveEntry entry1 = tai.getNextEntry();
        ArchiveEntry entry2 = tai.getNextEntry();
        if (entry1.getName().compareTo(entry2.getName()) > 0) { // kinda sort them lol
            ArchiveEntry tmp = entry1;
            entry1 = entry2;
            entry2 = tmp;
        }
        Assert.assertNotNull("No entry found in destination archive", entry1);
        Assert.assertEquals("Entry has wrong size", 5, entry1.getSize());
        System.out.println(entry1.getName());
        Assert.assertEquals("Entry has wrong relative path", src.getName() + "/1.txt", entry1.getName());
        System.out.println(entry2.getName());
        Assert.assertEquals("Entry has wrong size", 2, entry2.getSize());
        Assert.assertEquals("Entry has wrong relative path", src.getName() + "/2.txt", entry2.getName());
    }
}
