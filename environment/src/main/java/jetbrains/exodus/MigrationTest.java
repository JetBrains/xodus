/**
 * Copyright 2010 - 2022 JetBrains s.r.o.
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
package jetbrains.exodus;

import jetbrains.exodus.env.*;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;

public class MigrationTest {
    public static void main(String[] args) {
        try {
            final String envHome = "/home/andrey/old-db";
            final String dataFile = "/home/andrey/old-db/data-file";

            EnvironmentConfig config = new EnvironmentConfig();
            config.removeSetting(EnvironmentConfig.CIPHER_KEY);
            config.removeSetting(EnvironmentConfig.CIPHER_ID);

            try (final FileInputStream fileInputStream = new FileInputStream(dataFile)) {
                try (final DataInputStream dataInputStream = new DataInputStream(fileInputStream)) {
                    try (final Environment environment = Environments.newInstance(envHome, config)) {
                        environment.executeInReadonlyTransaction(txn -> {
                            System.out.println("Pre-check of storages");

                            var storeNames = environment.getAllStoreNames(txn);
                            for (var storeName : storeNames) {
                                var store = environment.openStore(storeName, StoreConfig.USE_EXISTING, txn);

                                try (var cursor = store.openCursor(txn)) {
                                    while (cursor.getNext()) {
                                        cursor.getKey();
                                        cursor.getValue();
                                    }
                                }

                                System.out.printf("Store `%s` was processed%n", storeName);
                            }

                            System.out.println("Check stored data");

                            int count = 0;
                            while (true) {
                                try {
                                    final int storeId = dataInputStream.read();
                                    if (storeId == -1) {
                                        break;
                                    }

                                    final int rem = storeId % 4;
                                    final Store store;
                                    if (rem == 0) {
                                        store = environment.openStore("store " + storeId,
                                                StoreConfig.WITHOUT_DUPLICATES,
                                                txn);
                                    } else if (rem == 1) {
                                        store = environment.openStore("store " + storeId,
                                                StoreConfig.WITH_DUPLICATES,
                                                txn);
                                    } else if (rem == 2) {
                                        store = environment.openStore("store " + storeId,
                                                StoreConfig.WITH_DUPLICATES_WITH_PREFIXING,
                                                txn);
                                    } else {
                                        store = environment.openStore("store " + storeId,
                                                StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING,
                                                txn);
                                    }

                                    final int keySize = dataInputStream.readInt();
                                    final byte[] key = new byte[keySize];

                                    dataInputStream.readFully(key);

                                    final int valueSize = dataInputStream.readInt();
                                    final byte[] value = new byte[valueSize];

                                    dataInputStream.readFully(value);

                                    ByteIterable storedValue = store.get(txn, new ArrayByteIterable(key));
                                    if (!new ArrayByteIterable(value).equals(storedValue)) {
                                        throw new RuntimeException("For store '" + store.getName() +
                                                "' and key '" + Arrays.toString(key) + " expected value is '" +
                                                Arrays.toString(value) + " but found value is '" + storedValue + "'.");
                                    }
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }

                                count++;
                                if (count % 10_000 == 0) {
                                    System.out.printf("%,d records were processed %n", count);
                                }
                            }
                        });
                    }
                }
            }

            System.out.println("Check of the storage was successfully completed.");
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
