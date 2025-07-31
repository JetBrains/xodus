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
package jetbrains.exodus.lucene2;

import jetbrains.exodus.env.EnvironmentConfig;
import jetbrains.exodus.env.Environments;
import org.apache.commons.lang3.RandomUtils;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.store.BaseDirectoryTestCase;
import org.apache.lucene.util.IOConsumer;
import org.apache.lucene.util.IORunnable;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static jetbrains.exodus.lucene2.XodusDirectoryBaseTest.DirOperation.*;

public abstract class XodusDirectoryBaseTest extends BaseDirectoryTestCase {

    private static final Logger log = LoggerFactory.getLogger(XodusDirectoryBaseTest.class);
    @Rule
    public final TestName name = new TestName();

    private static final int DEFAULT_THREADS = 8;
    private static final int DEFAULT_ROUNDS = 10;
    private static final int DEFAULT_INTERRUPTION_INTERVAL = 100;

    protected abstract EnvironmentConfig getEnvironmentConfig();

    protected Directory getDirectory(
            Path path,
            Consumer<Path> cleanupListener,
            Consumer<Long> ivGenListener,
            IORunnable beforeFileOperations
    ) throws IOException {
        return XodusCacheDirectory.fromXodusEnv(
                Environments.newInstance(
                        path.toFile(),
                        getEnvironmentConfig()
                ),
                cleanupListener, ivGenListener, beforeFileOperations
        );
    }

    @Override
    protected Directory getDirectory(Path path) throws IOException {
        return getDirectory(path, null, null, null);
    }

    @Test
    public void testDurableDir() throws IOException {

        final var fileCount = RandomUtils.nextInt(10, 1000);
        final var expectedContent = IntStream.range(0, fileCount)
                .boxed()
                .collect(Collectors.toMap(
                        i -> "file_" + i,
                        i -> RandomUtils.nextBytes(RandomUtils.nextInt(100, 100000))
                ));

        final var fileDir = createTempDir("testDurableDir");
        try (Directory dir = getDirectory(fileDir)) {
            for (Map.Entry<String, byte[]> e : expectedContent.entrySet()) {
                final var fileName = e.getKey();
                final var bytes = e.getValue();
                try (var o = dir.createOutput(fileName, newIOContext(random()))) {
                    o.writeBytes(bytes, bytes.length);
                }
            }
        }

        try (Directory dir = getDirectory(fileDir)) {
            checkDirContent(expectedContent, getDirectoryContent(dir), 0);
        }
    }

    @Test
    public void simple_createSameFile() throws IOException {
        // two files being created by 8 threads, only 2 win
        final var name1 = "name_1";
        final var name2 = "name_2";

        simpleScenarioTest(
                DEFAULT_ROUNDS, DEFAULT_THREADS,
                dir -> {
                },
                (dir, get, r, t, i) -> ignoreFileException(() ->
                        dir.createFile(t % 2 == 0 ? name1 : name2)
                ),
                DirOperationsVerifier.match(
                        isCreate(name1::equals),
                        isCreate(name2::equals)
                )
        );
    }

    @Test
    public void simple_deleteSameFile() throws IOException {
        // two files being deleted by 8 threads, only 2 win
        final var name1 = "name_1";
        final var name2 = "name_2";

        simpleScenarioTest(
                DEFAULT_ROUNDS, DEFAULT_THREADS,
                dir -> {
                    dir.createFile(name1);
                    dir.createFile(name2);
                },
                (dir, get, r, t, i) -> ignoreFileException(() ->
                        dir.deleteFile(t % 2 == 0 ? name1 : name2)
                ),
                DirOperationsVerifier.match(
                        isDelete(name1::equals),
                        isDelete(name2::equals)
                )
        );
    }

    @Test
    public void simple_renameSameFile() throws IOException {
        // same file being renamed by 8 threads, only 1 win
        final var oldFileName = "old_name";
        final var newNamePrefix = "new_name";
        simpleScenarioTest(
                DEFAULT_ROUNDS, DEFAULT_THREADS,
                dir -> dir.createFile(oldFileName),
                (dir, gen, r, t, i) -> ignoreFileException(() ->
                        dir.renameFile(oldFileName, newNamePrefix + t)
                ),
                DirOperationsVerifier.match(
                        isRename(oldFileName::equals, n -> n.startsWith(newNamePrefix))
                )
        );
    }

    @Test
    public void simple_renameIntoSameFile() throws IOException {
        // 8 files being renamed to the same name by 8 threads, only 1 wins
        final var oldFilePrefix = "old_name";
        final var newFileName = "new_name";
        final var numberOfThreads = DEFAULT_THREADS;
        simpleScenarioTest(
                DEFAULT_ROUNDS, numberOfThreads,
                dir -> dir.createNFiles(oldFilePrefix, numberOfThreads),
                (dir, gen, r, t, i) -> ignoreFileException(() ->
                        dir.renameFile(oldFilePrefix + t, newFileName)
                ),
                DirOperationsVerifier.match(
                        isRename(n -> n.startsWith(oldFilePrefix), newFileName::equals)
                )
        );
    }

    @Test
    public void simple_sameRename() throws IOException {
        // 8 threads doing the same rename operation, only 1 wins
        final var oldName = "old_name";
        final var newName = "new_name";
        simpleScenarioTest(
                DEFAULT_ROUNDS, DEFAULT_THREADS,
                dir -> dir.createFile(oldName),
                (dir, gen, r, t, i) -> ignoreFileException(() ->
                        dir.renameFile(oldName, newName)
                ),
                DirOperationsVerifier.match(
                        isRename(oldName::equals, newName::equals)
                )
        );
    }

    @Test
    public void simple_renameAndDelete() throws IOException {
        // 8 threads, attempting to rename or delete same file, only 1 wins (either delete or remove)
        final var oldName = "old_name";
        final var newName = "new_name";
        simpleScenarioTest(
                DEFAULT_ROUNDS, DEFAULT_THREADS,
                dir -> dir.createFile(oldName),
                (dir, gen, r, t, i) -> ignoreFileException(() -> {
                    if (t % 2 == 0) {
                        dir.deleteFile(oldName);
                    } else {
                        dir.renameFile(oldName, newName);
                    }
                }),
                DirOperationsVerifier.match(
                        isRename(oldName::equals, newName::equals).or(isDelete(oldName::equals))
                )
        );
    }


    @Test
    public void random_create() throws IOException {
        randomScenariosTest(
                Map.of(DirOperationType.CREATE, 1.0),
                () -> new RandomNameGenerator("file_", 200),
                DEFAULT_ROUNDS,
                0,
                DEFAULT_THREADS,
                30
        );
    }

    @Test
    public void random_createHighContention() throws IOException {
        // all threads are trying to create files with the same name
        randomScenariosTest(
                Map.of(DirOperationType.CREATE, 1.0),
                () -> new PooledNameGenerator("file_", 200),
                DEFAULT_ROUNDS,
                0,
                DEFAULT_THREADS,
                20
        );
    }

    @Test
    public void random_delete() throws IOException {
        randomScenariosTest(
                Map.of(DirOperationType.DELETE, 1.0),
                UniqueFileNameGenerator::new,
                DEFAULT_ROUNDS,
                80,
                DEFAULT_THREADS,
                10
        );
    }

    @Test
    public void random_rename() throws IOException {
        randomScenariosTest(
                Map.of(DirOperationType.RENAME, 1.0),
                () -> new RandomNameGenerator("file_", 200),
                DEFAULT_ROUNDS,
                50,
                DEFAULT_THREADS,
                10
        );
    }

    @Test
    public void random_renameHighContention() throws IOException {
        randomScenariosTest(
                Map.of(DirOperationType.RENAME, 1.0),
                () -> new PooledNameGenerator("file_", 200),
                DEFAULT_ROUNDS,
                50,
                DEFAULT_THREADS,
                10
        );
    }

    @Test
    public void random_deleteRename() throws IOException {
        randomScenariosTest(
                Map.of(
                        DirOperationType.DELETE, 0.5,
                        DirOperationType.RENAME, 0.5
                ),
                UniqueFileNameGenerator::new,
                DEFAULT_ROUNDS,
                500,
                DEFAULT_THREADS,
                100
        );
    }

    @Test
    public void random_createRenameDelete() throws IOException {
        randomScenariosTest(
                Map.of(
                        DirOperationType.CREATE, 0.4,
                        DirOperationType.DELETE, 0.3,
                        DirOperationType.RENAME, 0.3
                ),
                UniqueFileNameGenerator::new,
                DEFAULT_ROUNDS,
                0,
                DEFAULT_THREADS,
                100
        );
    }

    @Test
    public void crash_create() throws IOException {
        randomScenariosCrashTest(
                Map.of(DirOperationType.CREATE, 1.0),
                UniqueFileNameGenerator::new,
                DEFAULT_ROUNDS,
                0,
                DEFAULT_THREADS,
                10000,
                DEFAULT_INTERRUPTION_INTERVAL,
                DirContentVerifier.DIR_CAN_CONTAIN_MORE
        );
    }

    @Test
    public void crash_delete() throws IOException {
        randomScenariosCrashTest(
                Map.of(DirOperationType.DELETE, 1.0),
                UniqueFileNameGenerator::new,
                DEFAULT_ROUNDS,
                500,
                DEFAULT_THREADS,
                10000,
                DEFAULT_INTERRUPTION_INTERVAL,
                DirContentVerifier.DIR_CAN_CONTAIN_LESS
        );
    }

    @Test
    public void crash_rename() throws IOException {
        randomScenariosCrashTest(
                Map.of(DirOperationType.RENAME, 1.0),
                UniqueFileNameGenerator::new,
                DEFAULT_ROUNDS,
                500,
                DEFAULT_THREADS,
                10000,
                DEFAULT_INTERRUPTION_INTERVAL,
                (expected, actual) -> {
                    assertEquals(expected.size(), actual.size());
                    for (byte[] expectedValue : expected.values()) {
                        assertTrue(
                                actual.values().stream().anyMatch(actualValue -> Arrays.equals(expectedValue, actualValue))
                        );
                    }
                }
        );
    }

    private void multiThreadedTest(
            int numOfRounds,
            int numOfThreads,
            int numOfIterationsPerThread,
            Supplier<FileNameGenerator> fileNameGenerator,
            IOConsumer<DirectoryWrapper> dirInitializer,
            DirectoryWork work,
            long interruptAfterMillis,
            DirContentVerifier dirContentVerifier,
            DirOperationsVerifier opsVerifier
    ) throws IOException {
        final var testName = name.getMethodName();
        log.info(
                "Rounds: {}, threads: {}, iterationsPerThread: {}",
                numOfRounds, numOfThreads, numOfIterationsPerThread
        );

        try (var executor = Executors.newFixedThreadPool(numOfThreads)) {
            for (int r = 0; r < numOfRounds; r++) {
                final Map<String, byte[]> expectedDirContent;
                final Map<String, byte[]> actualDirContent;
                final List<DirOperation> performedOperations;
                final var fileDir = createTempDir(testName + "_" + r);
                final var fileNameGen = fileNameGenerator.get();

                try (var dir = getDirectory(
                        fileDir, null, null, XodusDirectoryBaseTest::checkThreadInterrupted
                )) {
                    final var dirWrapper = new DirectoryWrapper(dir);
                    dirInitializer.accept(dirWrapper);

                    dirWrapper.enableCollectingLog();

                    final var cb = new CyclicBarrier(numOfThreads);
                    final var futures = new ArrayList<Future<?>>();

                    for (int t = 0; t < numOfThreads; t++) {
                        final var roundId = r;
                        final var threadId = t;
                        futures.add(executor.submit(() -> {
                            try {
                                cb.await();
                                for (int i = 0; i < numOfIterationsPerThread; i++) {
                                    work.run(dirWrapper, fileNameGen, roundId, threadId, i);
                                }
                            } catch (Exception e) {
                                if (!(e instanceof InterruptedException) && !(e instanceof InterruptedIOException)) {
                                    log.error("Unexpected exception", e);
                                }
                                throw new RuntimeException(e);
                            }
                        }));
                    }

                    if (interruptAfterMillis <= 0) {
                        for (Future<?> f : futures) {
                            try {
                                f.get();
                            } catch (InterruptedException | ExecutionException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    } else {
                        sleep(interruptAfterMillis);
                        for (Future<?> f : futures) {
                            f.cancel(true);
                        }
                    }

                    expectedDirContent = dirWrapper.getContent();
                    performedOperations = dirWrapper.getOperationLog();
                    if (interruptAfterMillis <= 0) {
                        actualDirContent = getDirectoryContent(dir);
                    } else {
                        // it's unsafe to do this
                        actualDirContent = null;
                    }
                }

                final ArrayList<Path> cleanedFiles = new ArrayList<>();
                final Map<String, byte[]> dirContentAfterReopen;
                // repeat the check once again on a freshly opened directory
                try (var dir = getDirectory(fileDir, cleanedFiles::add, null, null)) {
                    dirContentAfterReopen = getDirectoryContent(dir);
                }
                if (!cleanedFiles.isEmpty()) {
                    if (interruptAfterMillis <= 0) {
                        fail("No cleanup was expected, but some files were cleaned up: " + cleanedFiles);
                    }
                }

                if (interruptAfterMillis <= 0) {
                    dirContentVerifier.verify(expectedDirContent, actualDirContent);
                    dirContentVerifier.verify(actualDirContent, dirContentAfterReopen);
                } else {
                    dirContentVerifier.verify(expectedDirContent, dirContentAfterReopen);

                }
                opsVerifier.verify(performedOperations);

                // in case of interruption, some files could be cleaned up and the directory state might have
                // changed in theory. we want to check it once again to be 100% sure
                if (interruptAfterMillis > 0) {
                    final ArrayList<Path> cleanedFiles2 = new ArrayList<>();
                    try (var dir = getDirectory(fileDir, cleanedFiles2::add, null, null)) {
                        DirContentVerifier.SHOULD_EQUAL.verify(dirContentAfterReopen, getDirectoryContent(dir));
                        assertTrue(
                                "No more cleanup expected, but some files were cleaned: " + cleanedFiles2,
                                cleanedFiles2.isEmpty()
                        );
                    }
                }
            }
        }
    }

    interface DirContentVerifier {
        void verify(
                Map<String, byte[]> expected,
                Map<String, byte[]> actual
        );

        DirContentVerifier SHOULD_EQUAL = (expected, actual) -> {
            checkDirContent(expected, actual, 0);
        };

        DirContentVerifier DIR_CAN_CONTAIN_MORE = (expected, actual) -> {
            logSizeMismatch(expected, actual);
            checkDirContent(expected, actual, -1);
        };

        DirContentVerifier DIR_CAN_CONTAIN_LESS = (expected, actual) -> {
            logSizeMismatch(expected, actual);
            checkDirContent(expected, actual, 1);
        };

        private static void logSizeMismatch(
                Map<String, byte[]> expected,
                Map<String, byte[]> actual
        ) {
            if (expected.size() != actual.size()) {
                log.info("expected: {}, after reopen: {}", expected.size(), actual.size());
            }
        }
    }

    interface DirOperationsVerifier {
        void verify(List<DirOperation> operations);

        @SafeVarargs
        static DirOperationsVerifier match(Predicate<DirOperation>... opMatchers) {
            return ops -> {
                final var errorMessage = "Operations on dir do not match the expected: " + ops;
                assertEquals(errorMessage, opMatchers.length, ops.size());
                for (Predicate<DirOperation> opMatcher : opMatchers) {
                    assertTrue(errorMessage, ops.stream().anyMatch(opMatcher));
                }
            };
        }

        DirOperationsVerifier IGNORE = ops -> {
        };
    }

    interface DirectoryWork {
        void run(
                DirectoryWrapper dir,
                FileNameGenerator fileNameGenerator,
                int roundId,
                int threadId,
                int iterationId
        ) throws IOException;
    }

    private void randomScenariosTest(
            Map<DirOperationType, Double> opDistribution,
            Supplier<FileNameGenerator> fileNameGenerator,
            int numOfRounds,
            int numOfInitialFiles,
            int numOfThreads,
            int numOfIterationsPerThread
    ) throws IOException {
        multiThreadedTest(
                numOfRounds, numOfThreads, numOfIterationsPerThread, fileNameGenerator,
                (dir) -> dir.createNFiles("initial_", numOfInitialFiles),
                (dir, fileNameGen, r, t, i) -> {
                    executeRandomAction(opDistribution, dir, fileNameGen, r, t, i);
                },
                -1,
                DirContentVerifier.SHOULD_EQUAL,
                DirOperationsVerifier.IGNORE
        );
    }

    private void randomScenariosCrashTest(
            Map<DirOperationType, Double> opDistribution,
            Supplier<FileNameGenerator> fileNameGenerator,
            int numOfRounds,
            int numOfInitialFiles,
            int numOfThreads,
            int numOfIterationsPerThread,
            int interruptAfterMillis,
            DirContentVerifier dirContentVerifier
    ) throws IOException {
        multiThreadedTest(
                numOfRounds, numOfThreads, numOfIterationsPerThread, fileNameGenerator,
                (dir) -> dir.createNFiles("initial_", numOfInitialFiles),
                (dir, fileNameGen, r, t, i) -> {
                    executeRandomAction(opDistribution, dir, fileNameGen, r, t, i);
                },
                interruptAfterMillis,
                dirContentVerifier,
                DirOperationsVerifier.IGNORE
        );
    }

    private static void executeRandomAction(Map<DirOperationType, Double> opDistribution, DirectoryWrapper dir, FileNameGenerator fileNameGen, int r, int t, int i) throws IOException {
        // choose an operation based on opDistribution probabilities
        switch (randomValueFromDistribution(opDistribution)) {
            case CREATE -> {
                try {
                    final var fileName = fileNameGen.peekName(r, t, i);
                    dir.createFile(fileName);
                    fileNameGen.nameHasBeenUsed(fileName);
                } catch (FileAlreadyExistsException e) {
                    // this is okay, ignoring it
                }
            }
            case RENAME -> retryOnFileException(() -> {
                final var oldName = randomValueFromCollection(dir.fileNames());

                if (oldName != null) {
                    final var newName = fileNameGen.peekName(r, t, i);
                    dir.renameFile(oldName, newName);
                    fileNameGen.nameHasBeenUsed(newName);
                }
            });

            case DELETE -> retryOnFileException(() -> {
                final var fileName = randomValueFromCollection(dir.fileNames());
                if (fileName != null) {
                    dir.deleteFile(fileName);
                }
            });
        }
    }

    private void simpleScenarioTest(
            int numOfRounds,
            int numOfThreads,
            IOConsumer<DirectoryWrapper> dirInitializer,
            DirectoryWork work,
            DirOperationsVerifier operationsVerifier
    ) throws IOException {
        multiThreadedTest(
                numOfRounds, numOfThreads, 1, UniqueFileNameGenerator::new,
                dirInitializer, work, -1, DirContentVerifier.SHOULD_EQUAL, operationsVerifier
        );
    }

    private static void retryOnFileException(IORunnable runnable) throws IOException {
        boolean retry = true;
        while (retry) {
            retry = false;
            try {
                runnable.run();
            } catch (Exception e) {
                if (e instanceof FileAlreadyExistsException || e instanceof NoSuchFileException) {
                    retry = true;
                } else {
                    throw e;
                }
            }
        }
    }

    private static void sleep(long maxSleep) {
        try {
            Thread.sleep(RandomUtils.nextLong(0, maxSleep));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static void ignoreFileException(IORunnable runnable) throws IOException {
        try {
            runnable.run();
        } catch (Exception e) {
            if (e instanceof FileAlreadyExistsException || e instanceof NoSuchFileException) {
                return;
            }
            throw e;
        }
    }

    private static byte[] createFile(
            Directory dir,
            String fileName,
            Function<String, byte[]> contentSupplier
    ) throws IOException {
        try (var out = dir.createOutput(fileName, newIOContext(random()))) {
            final var bytes = contentSupplier.apply(fileName);
            out.writeBytes(bytes, bytes.length);
            return bytes;
        }
    }

    protected static byte[] randomFileOfNPages(
            Directory dir,
            String fileName,
            int numOfPages
    ) throws IOException {
        return createFile(dir, fileName, fn -> {
            final var pageContentSize = ((XodusCacheDirectory) dir).getPageSize() - Long.BYTES;

            final var minContentSize = (numOfPages - 1) * pageContentSize + 1;
            final var maxContentSize = numOfPages * pageContentSize;

            final var length = RandomUtils.nextInt(minContentSize, maxContentSize + 1);

            return RandomUtils.nextBytes(length);
        });
    }

    private static byte[] randomFileOfSize(
            Directory dir,
            String fileName,
            int minSize,
            int maxSize
    ) throws IOException {
        return createFile(
                dir, fileName,
                fn -> RandomUtils.nextBytes(RandomUtils.nextInt(minSize, maxSize + 1))
        );
    }

    private static Map<String, byte[]> getDirectoryContent(Directory dir) throws IOException {
        final var dirContent = new HashMap<String, byte[]>();
        for (String fileName : dir.listAll()) {

            try (var i = dir.openInput(fileName, newIOContext(random()))) {

                final var fileBytes = new byte[(int) i.length()];
                i.readBytes(fileBytes, 0, fileBytes.length);

                dirContent.put(fileName, fileBytes);
            }
        }

        return Collections.unmodifiableMap(dirContent);
    }

    private static void checkDirContent(
            Map<String, byte[]> left,
            Map<String, byte[]> right,
            int comparison
    ) {

        if (comparison < 0) {
            final Set<String> leftNames = new HashSet<>(left.keySet());
            // left < right
            leftNames.removeAll(right.keySet());
            assertTrue(leftNames.isEmpty());
        } else if (comparison > 0) {
            final Set<String> rightNames = new HashSet<>(right.keySet());

            // left > right
            rightNames.removeAll(left.keySet());
            assertTrue(rightNames.isEmpty());
        } else {
            assertEquals(left.keySet(), right.keySet());
        }

        for (var e : left.entrySet()) {

            final var leftName = e.getKey();
            final var leftBytes = e.getValue();
            final var rightBytes = right.get(leftName);
            if (rightBytes != null) {
                assertArrayEquals(leftBytes, rightBytes);
            }
        }
    }

    private static <T> T randomValueFromCollection(Collection<T> col) {
        return col.stream().skip(RandomUtils.nextInt(0, col.size())).findFirst().orElse(null);
    }

    private static <T> T randomValueFromDistribution(Map<T, Double> distribution) {
        final var v = RandomUtils.nextDouble(0, 1);
        double sum = 0;
        for (var e : distribution.entrySet()) {
            sum += e.getValue();
            if (sum > v) {
                return e.getKey();
            }
        }
        throw new IllegalArgumentException("Distribution is invalid");
    }

    interface FileNameGenerator {
        String peekName(int roundIdx, int threadIdx, int iterationIdx);

        void nameHasBeenUsed(String name);
    }

    class PooledNameGenerator implements FileNameGenerator {
        private final Queue<String> namesPool;

        PooledNameGenerator(String prefix, int numberOfNames) {
            this(IntStream.range(0, numberOfNames)
                    .mapToObj(i -> prefix + i)
                    .collect(Collectors.toSet()));
        }

        PooledNameGenerator(Set<String> names) {
            final var namesList = new ArrayList<>(names);
            Collections.shuffle(namesList);
            this.namesPool = new ConcurrentLinkedQueue<>(namesList);
        }

        @Override
        public String peekName(int roundIdx, int threadIdx, int iterationIdx) {
            return namesPool.peek();
        }

        @Override
        public void nameHasBeenUsed(String name) {
            namesPool.remove();
        }
    }


    class RandomNameGenerator implements FileNameGenerator {
        private final String prefix;
        private final int maxIndex;

        RandomNameGenerator(String prefix, int maxIndex) {
            this.prefix = prefix;
            this.maxIndex = maxIndex;
        }


        @Override
        public String peekName(int roundIdx, int threadIdx, int iterationIdx) {
            return prefix + RandomUtils.nextInt(0, maxIndex);
        }

        @Override
        public void nameHasBeenUsed(String name) {
        }
    }

    static class UniqueFileNameGenerator implements FileNameGenerator {

        @Override
        public String peekName(int roundIdx, int threadIdx, int iterationIdx) {
            return "file_" + roundIdx + "_" + threadIdx + "_" + iterationIdx;
        }

        @Override
        public void nameHasBeenUsed(String name) {
        }
    }

    static class DirectoryWrapper {
        private final Directory dir;
        private final Map<String, byte[]> dirState = new ConcurrentHashMap<>();
        private volatile boolean collectLog = false;
        private final Queue<DirOperation> operationLog = new ConcurrentLinkedQueue<>();

        DirectoryWrapper(Directory dir) {
            this.dir = dir;
        }

        Map<String, byte[]> getContent() {
            return dirState;
        }

        public void enableCollectingLog() {
            this.collectLog = true;
        }

        public List<DirOperation> getOperationLog() {
            return operationLog.stream().toList();
        }

        void createNFiles(String prefix, int n) throws IOException {
            for (int i = 0; i < n; i++) {
                createFile(prefix + i);
            }
        }

        void createFile(String name) throws IOException {
            dirState.put(name, randomFileOfSize(dir, name, 10, 100));
            logOperation(DirOperation.create(name));
        }

        void renameFile(String oldName, String newName) throws IOException {

            dir.rename(oldName, newName);
            final var oldContent = dirState.remove(oldName);
            assertNotNull("Empty content for new file " + oldName, oldContent);
            dirState.put(newName, oldContent);
            logOperation(DirOperation.rename(oldName, newName));
        }

        void deleteFile(String fileName) throws IOException {
            dir.deleteFile(fileName);
            dirState.remove(fileName);
            logOperation(DirOperation.delete(fileName));
        }

        private void logOperation(DirOperation op) {
            if (collectLog) {
                operationLog.add(op);
            }
        }

        Set<String> fileNames() {
            return dirState.keySet();
        }
    }

    enum DirOperationType {CREATE, RENAME, DELETE}

    record DirOperation(
            DirOperationType type,
            String name,
            String oldName
    ) {
        static DirOperation create(String name) {
            return new DirOperation(DirOperationType.CREATE, name, null);
        }

        static DirOperation rename(String oldName, String newName) {
            return new DirOperation(DirOperationType.RENAME, newName, oldName);
        }

        static DirOperation delete(String name) {
            return new DirOperation(DirOperationType.DELETE, name, null);
        }

        private static Predicate<DirOperation> match(
                DirOperationType type,
                Predicate<String> nameMatch,
                Predicate<String> oldNameMatch
        ) {
            return p -> p.type == type &&
                    nameMatch.test(p.name) &&
                    oldNameMatch.test(p.oldName);
        }

        static Predicate<DirOperation> isRename(
                Predicate<String> oldNameMatch,
                Predicate<String> nameMatch
        ) {
            return match(DirOperationType.RENAME, nameMatch, oldNameMatch);
        }

        static Predicate<DirOperation> isDelete(Predicate<String> nameMatch) {
            return match(DirOperationType.DELETE, nameMatch, Objects::isNull);
        }

        static Predicate<DirOperation> isCreate(Predicate<String> nameMatch) {
            return match(DirOperationType.CREATE, nameMatch, Objects::isNull);
        }
    }

    private static void checkThreadInterrupted() throws InterruptedIOException {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedIOException();
        }
    }
}
