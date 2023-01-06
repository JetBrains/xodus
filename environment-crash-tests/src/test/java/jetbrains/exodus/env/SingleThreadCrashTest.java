package jetbrains.exodus.env;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteIterable;

import jetbrains.exodus.TestUtil;
import jetbrains.exodus.util.IOUtil;
import jetbrains.exodus.util.Random;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;

public class SingleThreadCrashTest {
    @Test
    public void simpleSingleThreadCrashTest() throws Exception {
        var tmpDir = TestUtil.createTempDir();
        var dbDir = Files.createTempDirectory(Path.of(tmpDir.toURI()), "db");

        var javaHome = System.getProperty("java.home");
        System.out.println(SingleThreadCrashTest.class.getSimpleName() + " : java home - " + javaHome +
                ", working directory - " + tmpDir.getAbsolutePath());
        var javaExec = Path.of(javaHome, "bin", "java");

        var processBuilder = new ProcessBuilder();

        var classPath = System.getProperty("java.class.path");

        processBuilder.command(javaExec.toAbsolutePath().toString(),
                "-cp", classPath,
                "-Dexodus.cipherId=" + System.getProperty("exodus.cipherId"),
                "-Dexodus.cipherKey=" + System.getProperty("exodus.cipherKey"),
                "-Dexodus.cipherBasicIV=" + System.getProperty("exodus.cipherBasicIV"),
                SingleThreadCrashTest.CodeRunner.class.getName(),
                dbDir.toAbsolutePath().toString(),
                tmpDir.getAbsolutePath());

        processBuilder.inheritIO();

        var process = processBuilder.start();

        System.out.println("Process started.");
        Thread.sleep(300_000);
        System.out.println("Shutdown issued.");

        Files.createFile(Path.of(tmpDir.toURI()).resolve("shutdown"));
        process.waitFor();
        System.out.println("Process finished.");

        System.out.println("Checking the database. DB folder : " + dbDir.toAbsolutePath());

        final long start = System.nanoTime();
        try (final Environment environment = Environments.newInstance(dbDir.toFile())) {
            final long end = System.nanoTime();

            System.out.printf("Open time %d ms %n", Long.valueOf((end - start) / 1_000_000));
            environment.executeInReadonlyTransaction(txn -> {
                var storeNames = environment.getAllStoreNames(txn);
                System.out.printf("%d stores were found.%n", Integer.valueOf(storeNames.size()));

                for (var storeName : storeNames) {
                    var store = environment.openStore(storeName, StoreConfig.USE_EXISTING, txn);

                    try (var cursor = store.openCursor(txn)) {
                        while (cursor.getNext()) {
                            cursor.getKey();
                            cursor.getValue();
                        }
                    }
                }
            });
        }

        System.out.println("Database check is completed.");

        IOUtil.deleteRecursively(tmpDir);
    }

    public static final class CodeRunner {
        public static void main(String[] args) {
            long storeIdGen = 0;
            final var stores = new LongOpenHashSet();

            final long seed = System.nanoTime();

            System.out.println(SingleThreadCrashTest.class.getSimpleName() + " runner seed : " + seed);

            final Random rnd = new Random(seed);
            final Random contentRnd = new Random(seed);

            System.out.println("Environment will be opened at " + args[0]);

            long operations = 0;
            long storesCreated = 0;
            long storesDeleted = 0;
            long entitiesAdded = 0;
            long entitiesUpdated = 0;
            long entitiesDeleted = 0;

            var shutdownFile = Path.of(args[1]).resolve("shutdown");
            try (final Environment environment = Environments.newInstance(args[0])) {
                createStore(environment, stores, storeIdGen++);
                storesCreated++;
                operations++;

                //noinspection InfiniteLoopStatement
                while (true) {
                    if (Files.exists(shutdownFile)) {
                        Runtime.getRuntime().halt(-1);
                    }
                    var operation = rnd.nextDouble();

                    var success = false;
                    if (operation < 0.001) {
                        createStore(environment, stores, storeIdGen++);
                        storesCreated++;
                        operations++;
                        success = true;
                    } else if (operation < 0.0015) {
                        if (deleteStore(environment, stores, contentRnd)) {
                            storesDeleted++;
                            operations++;
                            success = true;
                        }
                    } else if (operation < 0.5) {
                        if (addEntryToStore(environment, stores, contentRnd)) {
                            entitiesAdded++;
                            operations++;
                            success = true;
                        }
                    } else if (operation < 0.7) {
                        if (deleteEntryFromStore(environment, stores, contentRnd)) {
                            entitiesDeleted++;
                            operations++;
                            success = true;
                        }
                    } else {
                        if (updateEntryInStore(environment, stores, contentRnd)) {
                            entitiesUpdated++;
                            operations++;
                            success = true;
                        }
                    }

                    if (success && operations % 10_000 == 0) {
                        printStats(operations, storesCreated, storesDeleted, entitiesAdded, entitiesUpdated, entitiesDeleted);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace(System.err);
                System.err.flush();
            }
        }

        private static void printStats(long operations, long storesCreated, long storesDeleted, long entitiesAdded,
                                       long entitiesUpdated, long entitiesDeleted) {
            //noinspection AutoBoxing
            System.out.printf("%,d operations were performed. Stores created : %,d ," +
                            " stores deleted : %,d , entries deleted %,d , entries added %,d , entries updated %,d %n",
                    operations, storesCreated, storesDeleted, entitiesDeleted, entitiesAdded, entitiesUpdated);
        }

        private static boolean updateEntryInStore(final Environment environment, final LongOpenHashSet stores,
                                                  final Random contentRnd) {
            if (stores.isEmpty()) {
                return false;
            }

            final long storeId = choseRandomStore(stores, contentRnd);
            environment.executeInTransaction(txn -> {
                var store = environment.openStore(String.valueOf(storeId), StoreConfig.USE_EXISTING, txn);
                var keyToUpdate = chooseRandomKey(txn, store, contentRnd);

                if (keyToUpdate == null) {
                    return;
                }

                final int valueSize = contentRnd.nextInt(1024) + 1;
                var value = new byte[valueSize];

                contentRnd.nextBytes(value);
                store.put(txn, keyToUpdate, new ArrayByteIterable(value));
            });

            return true;
        }

        private static boolean deleteEntryFromStore(final Environment environment, final LongOpenHashSet stores,
                                                    final Random contentRnd) {
            if (stores.isEmpty()) {
                return false;
            }

            final long storeId = choseRandomStore(stores, contentRnd);
            environment.executeInTransaction(txn -> {
                var store = environment.openStore(String.valueOf(storeId), StoreConfig.USE_EXISTING, txn);

                ByteIterable keyToDelete = chooseRandomKey(txn, store, contentRnd);
                if (keyToDelete == null) {
                    return;
                }

                store.delete(txn, keyToDelete);
            });
            stores.remove(storeId);

            return true;
        }

        @Nullable
        private static ByteIterable chooseRandomKey(Transaction txn, Store store, final Random contentRnd) {
            if (store.count(txn) == 0) {
                return null;
            }

            ByteIterable selectedKey;
            try (var cursor = store.openCursor(txn)) {
                if (!cursor.getNext()) {
                    return null;
                }

                var first = cursor.getKey();
                cursor.getLast();

                cursor.getLast();
                var last = cursor.getKey();


                var firstBytes = first.getBytesUnsafe();
                var lastBytes = last.getBytesUnsafe();

                var keySize = chooseRandomInterval(first.getLength(), last.getLength(), contentRnd);
                var key = new byte[keySize];
                var lenBoundary = Math.min(first.getLength(), last.getLength());

                for (int i = 0; i < keySize; i++) {
                    if (i < lenBoundary) {
                        key[i] = (byte) chooseRandomInterval(firstBytes[i], lastBytes[i], contentRnd);
                    } else {
                        key[i] = (byte) contentRnd.next(8);
                    }
                }

                selectedKey = cursor.getSearchKeyRange(new ArrayByteIterable(key));
                if (selectedKey == null) {
                    cursor.getLast();
                    selectedKey = cursor.getKey();
                }
            }

            return selectedKey;
        }

        private static int chooseRandomInterval(int first, int second, final Random contentRnd) {
            if (second < first) {
                var tmp = second;

                second = first;
                first = tmp;
            }

            final int diff = second - first;
            if (diff == 0) {
                return first;
            }

            return first + contentRnd.nextInt(diff + 1);
        }

        private static boolean addEntryToStore(final Environment environment, final LongOpenHashSet stores, final Random contentRnd) {
            if (stores.isEmpty()) {
                return false;
            }

            final int keySize = contentRnd.nextInt(64) + 1;
            final int valueSize = contentRnd.nextInt(1024) + 1;

            final byte[] key = new byte[keySize];
            final byte[] value = new byte[valueSize];

            contentRnd.nextBytes(key);
            contentRnd.nextBytes(value);

            final long storeId = choseRandomStore(stores, contentRnd);

            environment.executeInTransaction(txn -> {
                var store = environment.openStore(String.valueOf(storeId), StoreConfig.USE_EXISTING, txn);
                store.put(txn, new ArrayByteIterable(key), new ArrayByteIterable(value));
            });

            return true;
        }

        private static boolean deleteStore(final Environment environment, final LongOpenHashSet stores, Random contentRnd) {
            if (stores.isEmpty()) {
                return false;
            }

            final long storeToRemove = choseRandomStore(stores, contentRnd);
            environment.executeInTransaction(txn -> environment.removeStore(String.valueOf(storeToRemove), txn));
            stores.remove(storeToRemove);
            return true;
        }

        private static long choseRandomStore(LongOpenHashSet stores, Random contentRnd) {
            final int storeIndex = contentRnd.nextInt(stores.size());
            var storeIterator = stores.iterator();
            long storeId = 0;

            for (int i = 0; i <= storeIndex; i++) {
                storeId = storeIterator.nextLong();
            }

            return storeId;
        }

        private static void createStore(final Environment environment, final LongOpenHashSet stores, final long storeId) {
            environment.executeInTransaction(tx ->
                    environment.openStore(String.valueOf(storeId), StoreConfig.WITHOUT_DUPLICATES, tx));
            stores.add(storeId);
        }
    }
}
