/**
 * Copyright 2010 - 2015 JetBrains s.r.o.
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
package jetbrains.exodus.env;

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ExodusException;
import jetbrains.exodus.bindings.LongBinding;
import jetbrains.exodus.core.dataStructures.hash.*;
import jetbrains.exodus.core.dataStructures.hash.LinkedHashSet;
import jetbrains.exodus.gc.GarbageCollector;
import jetbrains.exodus.io.FileDataReader;
import jetbrains.exodus.io.FileDataWriter;
import jetbrains.exodus.log.*;
import jetbrains.exodus.tree.ITree;
import jetbrains.exodus.tree.ITreeCursor;
import jetbrains.exodus.tree.LongIterator;
import jetbrains.exodus.tree.TreeMetaInfo;
import jetbrains.exodus.tree.btree.AddressIterator;
import jetbrains.exodus.tree.btree.BTree;
import jetbrains.exodus.tree.btree.BTreeBalancePolicy;
import jetbrains.exodus.tree.patricia.PatriciaTreeBase;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;

public class Reflect {

    private static final Logger logger = LoggerFactory.getLogger(Reflect.class);
    private static final int DEFAULT_PAGE_SIZE = LogUtil.LOG_BLOCK_ALIGNMENT * 16;
    private static final int MAX_VALID_LOGGABLE_TYPE = PatriciaTreeBase.MAX_VALID_LOGGABLE_TYPE;

    private final EnvironmentImpl env;
    private final Log log;

    public Reflect(@NotNull final File directory) {
        final File[] files = LogUtil.listFiles(directory);
        Arrays.sort(files, new Comparator<File>() {
            @Override
            public int compare(File o1, File o2) {
                long cmp = LogUtil.getAddress(o1.getName()) - LogUtil.getAddress(o2.getName());
                if (cmp > 0) return 1;
                if (cmp < 0) return -1;
                return 0;
            }
        });

        final int filesLength = files.length;
        if (filesLength == 0) {
            throw new IllegalArgumentException("No files found");
        }
        System.out.println("Files found: " + filesLength);

        int i = 0;
        long fileSize = 0;
        for (final File f : files) {
            ++i;
            final long length = f.length();
            if (fileSize % LogUtil.LOG_BLOCK_ALIGNMENT != 0 && i != filesLength) {
                throw new IllegalArgumentException("Length of non-last file " + f.getName() + " is badly aligned: " + length);
            }
            fileSize = Math.max(fileSize, length);
        }
        System.out.println("Max file length: " + fileSize);

        final int pageSize;
        if (fileSize % DEFAULT_PAGE_SIZE == 0 || filesLength == 1) {
            pageSize = DEFAULT_PAGE_SIZE;
        } else {
            pageSize = LogUtil.LOG_BLOCK_ALIGNMENT;
        }
        System.out.println("Computed page size: " + pageSize);

        env = (EnvironmentImpl) Environments.newInstance(
                LogConfig.create(new FileDataReader(directory, 16), new FileDataWriter(directory)),
                new EnvironmentConfig().setLogFileSize(((fileSize + pageSize - 1) / pageSize) * pageSize / LogUtil.LOG_BLOCK_ALIGNMENT).setLogCachePageSize(pageSize).setGcEnabled(false));
        log = env.getLog();
    }

    public List<DatabaseRoot> roots() {
        final List<DatabaseRoot> roots = new LinkedList<>();
        final long[] fileAddresses = log.getAllFileAddresses();
        for (int i = fileAddresses.length - 1; i >= 0; i--) {
            final long address = fileAddresses[i];
            final long endAddress = address + log.getFileSize(address);
            final Iterator<RandomAccessLoggable> itr = log.getLoggableIterator(address);
            while (itr.hasNext()) {
                final Loggable loggable = itr.next();
                if (loggable.getType() == DatabaseRoot.DATABASE_ROOT_TYPE) {
                    DatabaseRoot root = (DatabaseRoot) loggable;
                    if (root.isValid()) {
                        roots.add(root);
                    } else {
                        logger.error("Invalid root at address: " + loggable.getAddress());
                    }
                }
                if (loggable.getAddress() + loggable.length() >= endAddress) break;
            }
        }
        System.out.println("Roots found: " + roots.size());
        return roots;
    }

    public void traverseAll(List<DatabaseRoot> roots) {
        final LongSet traversed = new LongHashSet();
        final BTreeBalancePolicy strategy = env.getBTreeBalancePolicy();
        final MetaTree fallbackMetaTree = env.getMetaTree();
        final AbstractMap<Integer, Long> storeRoots = new IntHashMap<>();
        final int[] ids = new int[roots.size()]; // whatever
        int processed = 0;
        long totalLength = 0;
        for (final DatabaseRoot root : roots) {
            processed++;
            System.out.println(String.format("Processing root: %d, %d addresses traversed", processed, traversed.size()));
            int size = 0;
            final LongHashMap<TreeMetaInfo> meta = new LongHashMap<>();
            final BTree metaTree = new BTree(log, strategy, root.getRootAddress(), false, EnvironmentImpl.META_TREE_ID);
            try (ITreeCursor cursor = metaTree.openCursor()) {
                while (cursor.getNext()) {
                    try {
                        cursor.getNext();
                    } catch (ExodusException e) {
                        logger.error("Can't traverse meta tree");
                        break;
                    }
                    final ArrayByteIterable key = new ArrayByteIterable(cursor.getKey());
                    if (MetaTree.isStringKey(key)) {
                        final TreeMetaInfo metaInfo = TreeMetaInfo.load(env, cursor.getValue());
                        meta.put(metaInfo.getStructureId(), metaInfo);
                    } else {
                        final long address = CompressedUnsignedLongByteIterable.getLong(cursor.getValue());
                        final int structureId = (int) LongBinding.compressedEntryToLong(key);
                        final Long currentAddress = storeRoots.get(structureId);
                        if (currentAddress == null || currentAddress != address) {
                            ids[size++] = structureId;
                            storeRoots.put(structureId, address);
                        }
                    }
                }
            }
            for (int i = 0; i < size; ++i) {
                final int id = ids[i];
                final Long currentRoot = storeRoots.get(id);
                TreeMetaInfo currentInfo = meta.get(id);

                if (currentInfo == null) {
                    // try falling back to up-to-date meta-info
                    final String name = fallbackMetaTree.getStoreNameByStructureId(id, env);
                    if (name != null) {
                        currentInfo = fallbackMetaTree.getMetaInfo(name, env);
                    }
                    if (currentInfo == null) {
                        logger.error("No meta info for id: " + id);
                        continue;
                    }
                }

                try {
                    final BTree tree = new BTree(log, currentRoot, currentInfo.hasDuplicates(), id);

                    final AddressIterator itr = tree.addressIterator();

                    while (itr.hasNext()) {
                        final long address = itr.next();

                        if (traversed.contains(address)) {
                            itr.skipSubTree();
                        } else {
                            final RandomAccessLoggable loggable = log.read(address);
                            final int length = loggable.length();
                            totalLength += length;
                            traversed.add(address);
                        }
                    }
                } catch (BlockNotFoundException ignored) {
                }
            }
            System.out.println(String.format("Total data processed: %10.2fk", (double) totalLength / 1024));
        }
    }

    public void gatherLogStats() {
        final long[] fileAddresses = log.getAllFileAddresses();
        final IntHashMap<Integer> dataLengths = new IntHashMap<>();
        final IntHashMap<Integer> structureIds = new IntHashMap<>();
        final IntHashMap<Integer> types = new IntHashMap<>();
        int nullLoggables = 0;
        for (int i = fileAddresses.length - 1; i >= 0; ) {
            echoProgress("Gathering log statistics, reading file", fileAddresses.length, fileAddresses.length - i);
            final long address = fileAddresses[i--];
            final Iterator<RandomAccessLoggable> it = log.getLoggableIterator(address);
            while (it.hasNext()) {
                final Loggable loggable = it.next();
                final long la = loggable.getAddress();
                if (i >= 0 && la >= fileAddresses[i]) {
                    echoProgress("Gathering log statistics, reading file", fileAddresses.length, fileAddresses.length - i);
                    --i;
                }
                if (NullLoggable.isNullLoggable(loggable)) {
                    ++nullLoggables;
                } else {
                    inc(dataLengths, loggable.getDataLength());
                    inc(structureIds, (int) loggable.getStructureId());
                    inc(types, loggable.getType());
                }
            }
        }
        System.out.println();
        System.out.println();
        System.out.println("Null loggables: " + nullLoggables);
        dumpCounts("Data lengths:", dataLengths);
        dumpCounts("Structure ids:", structureIds);
        dumpCounts("Loggable types:", types);
    }

    private static void echoProgress(@NotNull final String message, final int total, final int current) {
        System.out.print("\r" + message + ' ' + current + " of " + total + ", " + ((current * 100) / total) + "% complete");
    }

    @SuppressWarnings("UnnecessaryBoxing")
    private static void inc(@NotNull final IntHashMap<Integer> counts, final int key) {
        final Integer count = counts.get(key);
        if (count == null) {
            counts.put(key, Integer.valueOf(1));
        } else {
            counts.put(key, Integer.valueOf(count + 1));
        }
    }

    private static void dumpCounts(@NotNull final String message, @NotNull final IntHashMap<Integer> counts) {
        System.out.println();
        System.out.println(message);
        final TreeSet<Integer> sortedKeys = new TreeSet<>(new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                final Integer count1 = counts.get(o1);
                final Integer count2 = counts.get(o2);
                if (count1 < count2) {
                    return 1;
                }
                if (count2 < count1) {
                    return -1;
                }
                return 0;
            }
        });
        sortedKeys.addAll(counts.keySet());
        for (final Integer key : sortedKeys) {
            System.out.print(key + ":" + counts.get(key) + ' ');
        }
        System.out.println();
    }

    public void traverse() {
        final TreeMap<Long, Long> usedSpace = new TreeMap<>();
        System.out.print("Analysing meta tree loggables... ");
        fetchUsedSpace(env.getMetaTree().addressIterator(), usedSpace);
        final List<String> names = env.computeInReadonlyTransaction(new TransactionalComputable<List<String>>() {
            @Override
            public List<String> compute(@NotNull Transaction txn) {
                return env.getAllStoreNames(txn);
            }
        });
        env.executeInReadonlyTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull final Transaction txn) {
                if (env.storeExists(GarbageCollector.UTILIZATION_PROFILE_STORE_NAME, txn)) {
                    names.add(GarbageCollector.UTILIZATION_PROFILE_STORE_NAME);
                }
            }
        });
        final int size = names.size();
        System.out.println("Done. Stores found: " + size);
        int i = 0;
        for (final String name : names) {
            System.out.println("Traversing store " + name + " (" + ++i + " of " + size + ')');
            try {
                env.executeInTransaction(new TransactionalExecutable() {
                    @Override
                    public void execute(@NotNull final Transaction txn) {
                        final StoreImpl store = env.openStore(name, StoreConfig.USE_EXISTING, txn);
                        int storeSize = 0;
                        try (Cursor cursor = store.openCursor(txn)) {
                            while (cursor.getNext()) {
                                ++storeSize;
                            }
                        }
                        final ITree tree = ((TransactionBase) txn).getTree(store);
                        fetchUsedSpace(tree.addressIterator(), usedSpace);
                        if (tree.getSize() != storeSize) {
                            logger.error("Stored size (" + tree.getSize() + ") isn't equal to actual size (" + storeSize + ')');
                        }
                    }
                });
            } catch (Throwable t) {
                System.out.println();
                logger.error("Can't fetch used space for store " + name, t);
            }
        }
        System.out.println();
        spaceInfo(usedSpace.entrySet());
    }

    public void copy(@NotNull final File there) {
        final Environment target = Environments.newInstance(there, env.getEnvironmentConfig());
        try {
            System.out.println("Copying environment to " + target.getLocation());
            final List<String> names = env.computeInReadonlyTransaction(new TransactionalComputable<List<String>>() {
                @Override
                public List<String> compute(@NotNull Transaction txn) {
                    return env.getAllStoreNames(txn);
                }
            });
            final int size = names.size();
            System.out.println("Stores found: " + size);

            int i = 0;
            for (final String name : names) {
                System.out.print("Copying store " + name + " (" + ++i + " of " + size + ')');
                final StoreConfig[] config = new StoreConfig[]{null};
                final long[] storeSize = new long[1];
                final Map<ByteIterable, Set<ByteIterable>> pairs = new TreeMap<>();
                final int[] totalPairs = {0};
                Throwable storeIsBroken = null;
                try {
                    env.executeInReadonlyTransaction(new TransactionalExecutable() {
                        @Override
                        public void execute(@NotNull final Transaction txn) {
                            final Store store = env.openStore(name, StoreConfig.USE_EXISTING, txn);
                            config[0] = store.getConfig();
                            storeSize[0] = store.count(txn);
                            try (Cursor cursor = store.openCursor(txn)) {
                                while (cursor.getNext()) {
                                    final ArrayByteIterable key = new ArrayByteIterable(cursor.getKey());
                                    Set<ByteIterable> valuesSet = pairs.get(key);
                                    if (valuesSet == null) {
                                        valuesSet = new TreeSet<>();
                                        pairs.put(key, valuesSet);
                                    }
                                    if (valuesSet.add(new ArrayByteIterable(cursor.getValue()))) {
                                        ++totalPairs[0];
                                    }
                                }
                            }
                        }
                    });
                } catch (Throwable t) {
                    storeIsBroken = t;
                }
                if (storeIsBroken != null) {
                    try {
                        env.executeInReadonlyTransaction(new TransactionalExecutable() {
                            @Override
                            public void execute(@NotNull final Transaction txn) {
                                final Store store = env.openStore(name, StoreConfig.USE_EXISTING, txn);
                                config[0] = store.getConfig();
                                storeSize[0] = store.count(txn);
                                try (Cursor cursor = store.openCursor(txn)) {
                                    while (cursor.getPrev()) {
                                        final ArrayByteIterable key = new ArrayByteIterable(cursor.getKey());
                                        Set<ByteIterable> valuesSet = pairs.get(key);
                                        if (valuesSet == null) {
                                            valuesSet = new TreeSet<>();
                                            pairs.put(key, valuesSet);
                                        }
                                        if (valuesSet.add(new ArrayByteIterable(cursor.getValue()))) {
                                            ++totalPairs[0];
                                        }
                                    }
                                }
                            }
                        });
                    } catch (Throwable ignore) {
                    }
                }
                if (storeIsBroken != null) {
                    System.out.println();
                    logger.error("Failed to completely copy store " + name, storeIsBroken);
                }
                System.out.println(". Saved store size = " + storeSize[0] + ", actual number of pairs = " + totalPairs[0]);
                target.executeInTransaction(new TransactionalExecutable() {
                    @Override
                    public void execute(@NotNull final Transaction txn) {
                        final Store store = target.openStore(name, config[0], txn);
                        for (final Map.Entry<ByteIterable, Set<ByteIterable>> pair : pairs.entrySet()) {
                            final ByteIterable key = pair.getKey();
                            final Set<ByteIterable> valueSet = pair.getValue();
                            for (final ByteIterable value : valueSet) {
                                store.putRight(txn, key, value);
                            }
                        }
                    }
                });
            }
        } finally {
            target.close();
        }
    }

    private void fetchUsedSpace(LongIterator itr, TreeMap<Long, Long> usedSpace) {
        while (itr.hasNext()) {
            try {
                final long address = itr.next();
                final RandomAccessLoggable loggable = log.read(address);
                final Long fileAddress = log.getFileAddress(address);
                Long usedBytes = usedSpace.get(fileAddress);
                if (usedBytes == null) {
                    usedBytes = 0L;
                }
                usedBytes += loggable.length();
                usedSpace.put(fileAddress, usedBytes);

                final int type = loggable.getType();
                if (type > MAX_VALID_LOGGABLE_TYPE) {
                    logger.error("Wrong loggable type: " + type);
                }
            } catch (ExodusException e) {
                logger.error("Can't enumerate loggable: " + e);
            }
        }
    }

    public void spaceInfo(Iterable<Map.Entry<Long, Long>> usedSpace) {
        for (final Map.Entry<Long, Long> pair : usedSpace) {
            long address = pair.getKey();
            final long size = log.getFileSize(address);
            if (size <= 0) {
                logger.error("Empty file unexpected");
            } else {
                final String msg;
                final Long usedBytes = pair.getValue();
                if (usedBytes == null) {
                    msg = ": unknown";
                } else {
                    msg = String.format(" %8.2fKB, %4.1f%%", (double) usedBytes / 1024, ((double) usedBytes / size) * 100);
                }
                System.out.println("Used bytes in file " + LogUtil.getLogFilename(address) + msg);
            }
        }
    }

    public void spaceInfoFromUtilization() {
        spaceInfo(new Iterable<Map.Entry<Long, Long>>() {
            private final long[] addresses = env.getLog().getAllFileAddresses();

            @Override
            public Iterator<Map.Entry<Long, Long>> iterator() {
                return new Iterator<Map.Entry<Long, Long>>() {
                    private int i = addresses.length - 1;

                    @Override
                    public boolean hasNext() {
                        return i >= 0;
                    }

                    @Override
                    public Map.Entry<Long, Long> next() {
                        final long key = addresses[i--];
                        final long fileFreeBytes = env.getGC().getFileFreeBytes(key);
                        final Long value = fileFreeBytes == Long.MAX_VALUE
                                ? null
                                : log.getFileSize(key) - fileFreeBytes;
                        return new Map.Entry<Long, Long>() {
                            @Override
                            public Long getKey() {
                                return key;
                            }

                            @Override
                            public Long getValue() {
                                return value;
                            }

                            @Override
                            public Long setValue(Long value) {
                                throw new UnsupportedOperationException();
                            }
                        };
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        });
    }

    public void cleanFile(@NotNull final String file) {
        env.suspendGC();
        try {
            env.getGC().doCleanFile(LogUtil.getAddress(file));
        } finally {
            env.resumeGC();
        }
    }

    public static void main(@NotNull final String[] args) throws Exception {
        if (args.length == 0) {
            printUsage();
            return;
        }
        String envPath = null;
        String envPath2 = null;
        boolean hasOptions = false;
        boolean gatherLogStats = false;
        boolean validateRoots = false;
        boolean traverse = false;
        boolean copy = false;
        boolean traverseAll = false;
        boolean utilizationInfo = false;
        final Set<String> files2Clean = new LinkedHashSet<>();
        for (final String arg : args) {
            if (arg.startsWith("-")) {
                hasOptions = true;
                if ("-ls".equalsIgnoreCase(arg)) {
                    gatherLogStats = true;
                } else if ("-r".equalsIgnoreCase(arg)) {
                    validateRoots = true;
                } else if ("-t".equalsIgnoreCase(arg)) {
                    traverse = true;
                } else if ("-c".equalsIgnoreCase(arg)) {
                    copy = true;
                } else if ("-ta".equalsIgnoreCase(arg)) {
                    traverseAll = true;
                } else if ("-u".equalsIgnoreCase(arg)) {
                    utilizationInfo = true;
                } else if (arg.toLowerCase().startsWith("-cl")) {
                    files2Clean.add(arg.substring(2));
                } else {
                    printUsage();
                    return;
                }
            } else {
                if (envPath == null) {
                    envPath = arg;
                } else {
                    envPath2 = arg;
                    break;
                }
            }
        }
        if (envPath == null || (copy && envPath2 == null)) {
            printUsage();
            return;
        }
        System.out.println("Started investigation of " + envPath);
        final Reflect reflect = new Reflect(new File(envPath));
        if (files2Clean.size() > 0) {
            for (final String file : files2Clean) {
                reflect.cleanFile(file);
            }
        }
        if (!hasOptions) {
            reflect.gatherLogStats();
            reflect.traverse();
        } else {
            List<DatabaseRoot> roots = null;
            if (validateRoots || traverseAll) {
                roots = reflect.roots();
            }
            if (gatherLogStats) {
                reflect.gatherLogStats();
            }
            if (traverse) {
                reflect.traverse();
            }
            if (copy) {
                reflect.copy(new File(envPath2));
            }
            if (utilizationInfo) {
                reflect.spaceInfoFromUtilization();
            }
            if (traverseAll) {
                reflect.traverseAll(roots);
            }
        }
    }

    private static void printUsage() {
        System.out.println("Usage: Reflect [-options] <environment path> [environment path 2]");
        System.out.println("Options:");
        System.out.println("  -ls             gather Log Stats");
        System.out.println("  -r              validate Roots");
        System.out.println("  -t              Traverse actual root");
        System.out.println("  -c              Copy actual root to a new environment (environment path 2 is mandatory)");
        System.out.println("  -ta             Traverse All roots, actual and expired (typically, would fail on environments created with GC)");
        System.out.println("  -u              display stored Utilization");
        System.out.println("  -cl<file name>  CLean particular file before any reflection");
    }
}