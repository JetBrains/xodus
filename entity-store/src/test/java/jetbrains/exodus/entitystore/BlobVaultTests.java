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
package jetbrains.exodus.entitystore;

import jetbrains.exodus.backup.BackupStrategy;
import jetbrains.exodus.backup.VirtualFileDescriptor;
import org.junit.Assert;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FilenameFilter;
import java.util.NavigableMap;
import java.util.TreeMap;

public class BlobVaultTests extends EntityStoreTestBase {

    public void testNewVaultFilesLocality() {
        final PersistentEntityStoreImpl store = getEntityStore();
        final PersistentStoreTransaction txn = getStoreTransaction();
        store.getConfig().setMaxInPlaceBlobSize(0); // no in-lace blobs
        for (int i = 0; i < 256; ++i) {
            txn.newEntity("E").setBlob("b", new ByteArrayInputStream("content".getBytes()));
        }
        Assert.assertTrue(txn.flush());
        final FileSystemBlobVaultOld blobVault = (FileSystemBlobVaultOld) store.getBlobVault().getSourceVault();
        final File vaultLocation = blobVault.getVaultLocation();
        Assert.assertEquals(257, vaultLocation.listFiles().length); // + "version" file
        Assert.assertEquals(256, vaultLocation.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(PersistentEntityStoreImpl.BLOBS_EXTENSION);
            }
        }).length);
        for (int i = 0; i < 256; ++i) {
            txn.newEntity("E").setBlob("b", new ByteArrayInputStream("content".getBytes()));
        }
        Assert.assertTrue(txn.flush());
        Assert.assertEquals(258, vaultLocation.listFiles().length);
        Assert.assertEquals(256, vaultLocation.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(PersistentEntityStoreImpl.BLOBS_EXTENSION);
            }
        }).length);
    }

    public void testHandleToFileAndFileToHandle() {
        final PersistentEntityStoreImpl store = getEntityStore();
        final PersistentStoreTransaction txn = getStoreTransaction();
        store.getConfig().setMaxInPlaceBlobSize(0); // no in-lace blobs
        final int count = 1000;
        for (int i = 0; i < count; ++i) {
            txn.newEntity("E").setBlob("b", new ByteArrayInputStream("content".getBytes()));
        }
        Assert.assertTrue(txn.flush());
        final FileSystemBlobVault blobVault = (FileSystemBlobVault) store.getBlobVault().getSourceVault();
        final NavigableMap<Long, File> handlesToFiles = new TreeMap<>();
        for (final VirtualFileDescriptor fd : blobVault.getBackupStrategy().getContents()) {
            final File file = ((BackupStrategy.FileDescriptor) fd).getFile();
            if (file.isFile() && !file.getName().equals(FileSystemBlobVaultOld.VERSION_FILE)) {
                final long handle = blobVault.getBlobHandleByFile(file);
                Assert.assertFalse(handlesToFiles.containsKey(handle));
                handlesToFiles.put(handle, file);
                Assert.assertEquals(file, blobVault.getBlobLocation(handle));
            }
        }
        final long min = handlesToFiles.navigableKeySet().iterator().next();
        Assert.assertEquals(0L, min);
        final long max = handlesToFiles.descendingKeySet().iterator().next();
        Assert.assertEquals((long) (count - 1), max);
    }
}
