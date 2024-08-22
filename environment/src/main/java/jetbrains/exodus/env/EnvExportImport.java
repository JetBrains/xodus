package jetbrains.exodus.env;

import jetbrains.exodus.ArrayByteIterable;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class EnvExportImport {
    public static void exportEnvironment(Path envPath, EnvironmentConfig envConf, Path exportPath) {
        try (Environment env = Environments.newInstance(envPath.toFile(), envConf)) {
            env.executeInReadonlyTransaction(txn -> {
                if (Files.exists(exportPath)) {
                    throw new IllegalStateException("Export file already exists: " + exportPath);
                }

                Path parent = exportPath.getParent();
                if (parent != null) {
                    try {
                        Files.createDirectories(parent);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
                try {
                    Files.createFile(exportPath);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                try {
                    try (OutputStream outputStream = Files.newOutputStream(exportPath)) {
                        try (DataOutputStream dataOutputStream = new DataOutputStream(outputStream)) {
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
    }

    public static void importEnvironment(Path envPath, EnvironmentConfig envConf, Path importPath) {
        try (Environment env = Environments.newInstance(envPath.toFile(), envConf)) {
            env.executeInTransaction(txn -> {
                try {
                    try (InputStream inputStream = Files.newInputStream(importPath)) {
                        try (DataInputStream dataInputStream = new DataInputStream(inputStream)) {
                            String currentStoreName = null;
                            Store currentStore = null;
                            int entryCount = 0;
                            try {
                                while (true) {
                                    final String storeName = dataInputStream.readUTF();
                                    String configName = dataInputStream.readUTF();

                                    final StoreConfig storeConfig = StoreConfig.valueOf(configName);
                                    if (currentStoreName == null) {
                                        currentStoreName = storeName;
                                        currentStore = env.openStore(storeName, storeConfig, txn);
                                    } else if (!currentStoreName.equals(storeName)) {
                                        System.out.println("Imported " + entryCount + " entries to store " + currentStoreName);

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
                                System.out.println("Imported " + entryCount + " entries to store " + currentStoreName);
                            }
                        }
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }
}
