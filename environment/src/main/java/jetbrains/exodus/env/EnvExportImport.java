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
package jetbrains.exodus.env;

import jetbrains.exodus.ArrayByteIterable;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.zip.CRC32;

public class EnvExportImport {
    public static void exportEnvironment(Path envPath, EnvironmentConfig envConf, Path exportPath) throws IOException {
        System.out.printf("Exporting database located in path: %s into file: %s%n", envPath, exportPath);
        if (Files.exists(exportPath)) {
            throw new IllegalStateException("File already exists in path : " + exportPath);
        }
        if (!Files.exists(envPath)) {
            throw new IllegalStateException("Database does not exist in path: " + envPath);
        }

        Path parent = exportPath.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Files.createFile(exportPath);

        int[] totalEntriesCount = new int[]{0};
        try (Environment env = Environments.newInstance(envPath.toFile(), envConf)) {
            env.executeInReadonlyTransaction(txn -> {
                try {
                    try (OutputStream outputStream = Files.newOutputStream(exportPath)) {
                        try (DataOutputStream dataOutputStream = new DataOutputStream(outputStream)) {
                            dataOutputStream.writeChar('V');
                            dataOutputStream.writeChar('S');

                            final List<String> stores = env.getAllStoreNames(txn);
                            for (String storeName : stores) {
                                final Store store = env.openStore(storeName, StoreConfig.USE_EXISTING, txn);

                                int entryCount = 0;
                                try (Cursor cursor = store.openCursor(txn)) {
                                    while (cursor.getNext()) {
                                        dataOutputStream.writeUTF(storeName);
                                        dataOutputStream.writeUTF(store.getConfig().name());


                                        byte[] key = cursor.getKey().getBytesUnsafe();

                                        dataOutputStream.writeInt(cursor.getKey().getLength());
                                        dataOutputStream.writeInt(key.length);
                                        dataOutputStream.write(key);

                                        byte[] value = cursor.getValue().getBytesUnsafe();

                                        dataOutputStream.writeInt(cursor.getValue().getLength());
                                        dataOutputStream.writeInt(value.length);

                                        dataOutputStream.write(value);

                                        entryCount++;
                                        totalEntriesCount[0]++;
                                    }
                                }

                                System.out.println("Exported " + entryCount + " entries from store " + storeName);
                            }
                        }
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        try (FileChannel fileChannel = FileChannel.open(exportPath, StandardOpenOption.READ,
                StandardOpenOption.WRITE)) {
            ByteBuffer totalEntriesCountBuffer = ByteBuffer.allocate(Integer.BYTES).putInt(totalEntriesCount[0]);
            totalEntriesCountBuffer.flip();

            fileChannel.position(fileChannel.size());
            int written = 0;
            while (written < Integer.BYTES) {
                written += fileChannel.write(totalEntriesCountBuffer);
            }

            final ByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
            CRC32 crc32 = new CRC32();
            crc32.update(buffer);

            fileChannel.position(fileChannel.size());
            long fileCrc = crc32.getValue();
            ByteBuffer crcBuffer = ByteBuffer.allocate(Long.BYTES).putLong(fileCrc);
            crcBuffer.flip();

            written = 0;
            while (written < Long.BYTES) {
                written += fileChannel.write(crcBuffer);
            }

            fileChannel.force(true);
        }

        System.out.printf("Export complete. %d entries were exported from database located in path: %s into file: %s%n",
                totalEntriesCount[0], envPath, exportPath);
    }

    public static void importEnvironment(Path envPath, EnvironmentConfig envConf, Path importPath) throws IOException {
        if (Files.exists(envPath)) {
            throw new IllegalStateException("Database already exists in path : " + envPath);
        }
        if (!Files.exists(importPath)) {
            throw new IllegalStateException("Import file does not exist in path : " + importPath);
        }

        System.out.printf("Importing database located in file: %s into database located into path: %s%n", importPath, envPath);
        int totalEntriesCount;

        try (FileChannel fileChannel = FileChannel.open(importPath, StandardOpenOption.READ)) {
            fileChannel.position(fileChannel.size() - Integer.BYTES - Long.BYTES);
            ByteBuffer totalEntriesCountBuffer = ByteBuffer.allocate(Integer.BYTES);
            int read = 0;
            while (read < Integer.BYTES) {
                int r = fileChannel.read(totalEntriesCountBuffer);
                if (r < 0) {
                    throw new EOFException(importPath + " - unexpected end of file.");
                }

                read += r;
            }
            totalEntriesCountBuffer.flip();
            totalEntriesCount = totalEntriesCountBuffer.getInt();

            final ByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0,
                    fileChannel.size() - Long.BYTES);

            CRC32 crc32 = new CRC32();
            crc32.update(buffer);
            long fileCrc = crc32.getValue();

            fileChannel.position(fileChannel.size() - Long.BYTES);
            ByteBuffer crcBuffer = ByteBuffer.allocate(Long.BYTES);

            read = 0;
            while (read < Long.BYTES) {
                int r = fileChannel.read(crcBuffer);
                if (r < 0) {
                    throw new EOFException(importPath + " - unexpected end of file");
                }

                read += r;
            }

            crcBuffer.flip();
            if (fileCrc != crcBuffer.getLong()) {
                throw new IllegalStateException("Import file : " + importPath + " is broken, CRC mismatch.");
            }
        }

        System.out.printf("Importing %d entries from file: %s into database located in path: %s%n", totalEntriesCount,
                importPath, envPath);

        try (Environment env = Environments.newInstance(envPath.toFile(), envConf)) {
            try {
                try (InputStream inputStream = Files.newInputStream(importPath)) {
                    try (DataInputStream dataInputStream = new DataInputStream(inputStream)) {
                        char magic1 = dataInputStream.readChar();
                        char magic2 = dataInputStream.readChar();

                        if (magic1 != 'V' || magic2 != 'S') {
                            throw new IllegalStateException(importPath + " - invalid export file format");
                        }

                        String currentStoreName = null;
                        Store currentStore = null;
                        int entryCount = 0;
                        Transaction txn = null;
                        try {
                            for (int i = 0; i < totalEntriesCount; i++) {
                                final String storeName = dataInputStream.readUTF();
                                String configName = dataInputStream.readUTF();

                                final StoreConfig storeConfig = StoreConfig.valueOf(configName);
                                if (currentStoreName == null) {
                                    currentStoreName = storeName;
                                    txn = env.beginTransaction();
                                    currentStore = env.openStore(storeName, storeConfig, txn);
                                } else if (!currentStoreName.equals(storeName)) {
                                    System.out.println("Imported " + entryCount + " entries to store " + currentStoreName);

                                    if (!txn.commit()) {
                                        throw new IllegalStateException("Failed to commit transaction for store "
                                                + currentStoreName);
                                    }

                                    txn = env.beginTransaction();
                                    currentStoreName = storeName;
                                    currentStore = env.openStore(storeName, storeConfig, txn);
                                    entryCount = 0;
                                }

                                final int keyLength = dataInputStream.readInt();
                                final int rawKeyLength = dataInputStream.readInt();
                                final byte[] key = new byte[rawKeyLength];
                                dataInputStream.readFully(key);

                                final int valueLength = dataInputStream.readInt();
                                final int rawValueLength = dataInputStream.readInt();
                                final byte[] value = new byte[rawValueLength];
                                dataInputStream.readFully(value);

                                currentStore.put(txn, new ArrayByteIterable(key, keyLength),
                                        new ArrayByteIterable(value, valueLength));
                                entryCount += 1;
                            }
                        } catch (EOFException e) {
                            // end of file
                        }
                        if (currentStoreName != null) {
                            if (!txn.isFinished()) {
                                if (!txn.commit()) {
                                    throw new IllegalStateException("Failed to commit transaction for store "
                                            + currentStoreName);
                                }
                            }

                            System.out.println("Imported " + entryCount + " entries to store " + currentStoreName);
                        }
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        System.out.printf("Import complete. Data located in file: %s imported to database located in path: %s%n", importPath,
                envPath);
    }
}
