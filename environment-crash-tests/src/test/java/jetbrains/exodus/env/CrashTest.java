package jetbrains.exodus.env;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteIterable;

import jetbrains.exodus.TestUtil;
import jetbrains.exodus.util.IOUtil;
import jetbrains.exodus.util.Random;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class CrashTest {
    @Test
    public void crashTest() throws Exception {
        //noinspection InfiniteLoopStatement
        while (true) {
            doCrashTest();
        }
    }

    private static void doCrashTest() throws IOException, InterruptedException {
        var tmpDir = TestUtil.createTempDir();
        var dbDir = Files.createTempDirectory(Path.of(tmpDir.toURI()), "db");

        var javaHome = System.getProperty("java.home");
        System.out.println(CrashTest.class.getSimpleName() + " : java home - " + javaHome +
                ", working directory - " + tmpDir.getAbsolutePath());
        var javaExec = Path.of(javaHome, "bin", "java");

        var processBuilder = new ProcessBuilder();

        var classPath = System.getProperty("java.class.path");

        final long seed = System.nanoTime();
        System.out.println(CrashTest.class.getSimpleName() + " runner seed : " + seed);

        processBuilder.command(javaExec.toAbsolutePath().toString(),
                "-cp", classPath,
                "-Dexodus.cipherId=" + System.getProperty("exodus.cipherId"),
                "-Dexodus.cipherKey=" + System.getProperty("exodus.cipherKey"),
                "-Dexodus.cipherBasicIV=" + System.getProperty("exodus.cipherBasicIV"),
                CodeRunner.class.getName(),
                dbDir.toAbsolutePath().toString(),
                tmpDir.getAbsolutePath(),
                String.valueOf(seed));

        processBuilder.inheritIO();

        var process = processBuilder.start();

        final Random rnd = new Random(seed);
        System.out.println("Process started.");
        final long shutdownTime = rnd.nextInt(15 * 60 * 1_000) + 1_000;

        System.out.printf("Time till application halt %,d  seconds.%n", Long.valueOf(shutdownTime / 1_000));
        Thread.sleep(shutdownTime);

        System.out.println("Shutdown issued.");

        Files.createFile(Path.of(tmpDir.toURI()).resolve("shutdown"));
        process.waitFor();
        System.out.println("Process finished.");

        System.out.println("Checking the database. DB folder : " + dbDir.toAbsolutePath());

        final long start = System.nanoTime();
        int entries;

        try (final Environment environment = Environments.newInstance(dbDir.toFile())) {
            final long end = System.nanoTime();

            System.out.printf("Open time %d ms %n", Long.valueOf((end - start) / 1_000_000));

            entries = environment.computeInReadonlyTransaction(txn -> {
                var entriesCount = 0;

                var storeNames = environment.getAllStoreNames(txn);
                System.out.printf("%d stores were found.%n", Integer.valueOf(storeNames.size()));

                for (var storeName : storeNames) {
                    var store = environment.openStore(storeName, StoreConfig.USE_EXISTING, txn);

                    try (var cursor = store.openCursor(txn)) {
                        while (cursor.getNext()) {
                            cursor.getKey();
                            cursor.getValue();
                            entriesCount++;
                        }
                    }
                }

                return Integer.valueOf(entriesCount);
            }).intValue();
        }

        System.out.printf("%,d entries were checked. Database check is completed.%n", Integer.valueOf(entries));

        IOUtil.deleteRecursively(tmpDir);
    }

    public static final class CodeRunner {
        public static void main(String[] args) {
            final long[] storeIdGen = new long[1];
            final var stores = new LongOpenHashSet();

            final long seed = Long.parseLong(args[2]);

            final Random rnd = new Random(seed);
            final Random contentRnd = new Random(seed);

            System.out.println("Environment will be opened at " + args[0]);

            boolean[] haltIssued = new boolean[1];

            var shutdownFile = Path.of(args[1]).resolve("shutdown");
            try (final Environment environment = Environments.newInstance(args[0])) {
                environment.executeInTransaction(txn -> createStore(environment, txn, stores, storeIdGen[0]++));

                //noinspection InfiniteLoopStatement
                while (true) {
                    var operationsInTx = rnd.nextInt(100) + 1;

                    environment.executeInTransaction(txn -> {
                        for (int i = 0; i < operationsInTx; i++) {
                            haltIssued[0] = checkHaltSignal(contentRnd, haltIssued[0], shutdownFile);
                            var operation = rnd.nextDouble();

                            if (operation < 0.001 && stores.size() < 1_000) {
                                createStore(environment, txn, stores, storeIdGen[0]++);
                            } else if (operation < 0.0015 && stores.size() >= 100) {
                                deleteStore(environment, txn, stores, contentRnd);
                            } else if (operation < 0.5) {
                                addEntryToStore(environment, txn, stores, contentRnd);
                            } else if (operation < 0.7) {
                                deleteEntryFromStore(environment, txn, stores, contentRnd);
                            } else {
                                updateEntryInStore(environment, txn, stores, contentRnd);
                            }
                        }
                    });
                }
            } catch (Exception e) {
                e.printStackTrace(System.err);
                System.err.flush();
            }
        }

        private static boolean checkHaltSignal(Random rnd, boolean haltIssued, Path shutdownFile) {
            if (!haltIssued && Files.exists(shutdownFile)) {
                var haltDelay = rnd.nextInt(10_000);
                var haltThread = new Thread(() -> {
                    try {
                        Thread.sleep(haltDelay);
                    } catch (InterruptedException e) {
                        //ignore
                    }

                    Runtime.getRuntime().halt(1);
                });

                haltThread.start();

                haltIssued = true;
            }
            return haltIssued;
        }


        private static void updateEntryInStore(final Environment environment,
                                               final Transaction txn,
                                               final LongOpenHashSet stores,
                                               final Random contentRnd) {
            if (stores.isEmpty()) {
                return;
            }

            final long storeId = choseRandomStore(stores, contentRnd);
            var store = environment.openStore(String.valueOf(storeId), StoreConfig.USE_EXISTING, txn);
            var keyToUpdate = chooseRandomKey(txn, store, contentRnd);

            if (keyToUpdate == null) {
                return;
            }

            final int valueSize = contentRnd.nextInt(1024) + 1;
            var value = new byte[valueSize];

            contentRnd.nextBytes(value);
            store.put(txn, keyToUpdate, new ArrayByteIterable(value));

        }

        private static void deleteEntryFromStore(final Environment environment, final Transaction txn,
                                                 final LongOpenHashSet stores,
                                                 final Random contentRnd) {
            if (stores.isEmpty()) {
                return;
            }

            final long storeId = choseRandomStore(stores, contentRnd);

            var store = environment.openStore(String.valueOf(storeId), StoreConfig.USE_EXISTING, txn);

            ByteIterable keyToDelete = chooseRandomKey(txn, store, contentRnd);
            if (keyToDelete == null) {
                return;
            }

            store.delete(txn, keyToDelete);

            stores.remove(storeId);

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

        private static void addEntryToStore(final Environment environment, final Transaction txn, final LongOpenHashSet stores, final Random contentRnd) {
            if (stores.isEmpty()) {
                return;
            }

            final int keySize = contentRnd.nextInt(64) + 1;
            final int valueSize = contentRnd.nextInt(1024) + 1;

            final byte[] key = new byte[keySize];
            final byte[] value = new byte[valueSize];

            contentRnd.nextBytes(key);
            contentRnd.nextBytes(value);

            final long storeId = choseRandomStore(stores, contentRnd);

            var store = environment.openStore(String.valueOf(storeId), StoreConfig.USE_EXISTING, txn);
            store.put(txn, new ArrayByteIterable(key), new ArrayByteIterable(value));
        }

        private static void deleteStore(final Environment environment, final Transaction txn,
                                        final LongOpenHashSet stores, Random contentRnd) {
            if (stores.isEmpty()) {
                return;
            }

            final long storeToRemove = choseRandomStore(stores, contentRnd);
            environment.removeStore(String.valueOf(storeToRemove), txn);
            stores.remove(storeToRemove);
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

        private static void createStore(final Environment environment, Transaction txn, final LongOpenHashSet stores, final long storeId) {
            environment.openStore(String.valueOf(storeId), StoreConfig.WITHOUT_DUPLICATES, txn);
            stores.add(storeId);
        }
    }
}
