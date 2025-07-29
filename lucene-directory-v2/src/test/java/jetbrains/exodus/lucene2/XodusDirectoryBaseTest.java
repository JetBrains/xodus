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
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.store.BaseDirectoryTestCase;
import org.apache.lucene.util.IORunnable;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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

public abstract class XodusDirectoryBaseTest extends BaseDirectoryTestCase {

    private static final Logger log = LoggerFactory.getLogger(XodusDirectoryBaseTest.class);
    @Rule
    public final TestName name = new TestName();

    protected abstract EnvironmentConfig getEnvironmentConfig();

    protected Directory getDirectory(
            Path path,
            Consumer<Path> cleanupListener,
            Consumer<Long> ivGenListener
    ) throws IOException {
        return XodusNonXodusDirectory.fromXodusEnv(
                Environments.newInstance(
                        path.toFile(),
                        getEnvironmentConfig()
                ),
                cleanupListener, ivGenListener
        );
    }

    @Override
    protected Directory getDirectory(Path path) throws IOException {
        return getDirectory(path, null, null);
    }

    @Test
    public void testDurableDir() throws IOException {

        final var fileCount = RandomUtils.nextInt(10, 1000);
        final var content = IntStream.range(0, fileCount)
                .boxed()
                .collect(Collectors.toMap(
                        i -> "file_" + i,
                        i -> RandomUtils.nextBytes(RandomUtils.nextInt(100, 100000))
                ));

        final var fileDir = createTempDir("testDurableDir");
        try (Directory dir = getDirectory(fileDir)) {
            for (Map.Entry<String, byte[]> e : content.entrySet()) {
                final var fileName = e.getKey();
                final var bytes = e.getValue();
                try (var o = dir.createOutput(fileName, newIOContext(random()))) {
                    o.writeBytes(bytes, bytes.length);
                }
            }
        }

        try (Directory dir = getDirectory(fileDir)) {
            validateDirectoryContent(dir, content);
        }
    }


    private enum DirOperation {CREATE, RENAME, DELETE}


    @Test
    public void testParallelCreate() throws IOException, ExecutionException, InterruptedException {
        runMultiThreadedRandomTest(
                "testParallelCreate",
                Map.of(DirOperation.CREATE, 1.0),
                () -> new RandomNameGenerator("file_", 200),
                8,
                0,
                20,
                30
        );
    }

    @Test
    public void testParallelCreateHighContention() throws IOException, ExecutionException, InterruptedException {
        // all threads are trying to create files with the same name
        runMultiThreadedRandomTest(
                "testParallelCreateHighContention",
                Map.of(DirOperation.CREATE, 1.0),
                () -> new PooledNameGenerator("file_", 200),
                8,
                0,
                30,
                20
        );
    }

    @Test
    public void testParallelDelete() throws IOException, ExecutionException, InterruptedException {
        runMultiThreadedRandomTest(
                "testParallelDelete",
                Map.of(DirOperation.DELETE, 1.0),
                UniqueFileNameGenerator::new, // not used in this test
                8,
                80,
                20,
                10
        );
    }

    @Test
    public void testParallelRename() throws IOException, ExecutionException, InterruptedException {
        runMultiThreadedRandomTest(
                "TestParallelRename",
                Map.of(DirOperation.RENAME, 1.0),
                () -> new RandomNameGenerator("file_", 200),
                8,
                50,
                20,
                10
        );
    }

    @Test
    public void testParallelRenameHighContention() throws IOException, ExecutionException, InterruptedException {
        runMultiThreadedRandomTest(
                "TestParallelRename",
                Map.of(DirOperation.RENAME, 1.0),
                () -> new PooledNameGenerator("file_", 200),
                8,
                50,
                20,
                10
        );
    }

    @Test
    public void testParallelDeleteRename() throws IOException, ExecutionException, InterruptedException {
        runMultiThreadedRandomTest(
                "testParallelDeleteRename",
                Map.of(
                        DirOperation.DELETE, 0.5,
                        DirOperation.RENAME, 0.5
                ),
                UniqueFileNameGenerator::new,
                8,
                500,
                20,
                100
        );
    }

    @Test
    public void testParallelWork() throws IOException, ExecutionException, InterruptedException {
        runMultiThreadedRandomTest(
                "testParallelWork",
                Map.of(
                        DirOperation.CREATE, 0.4,
                        DirOperation.DELETE, 0.3,
                        DirOperation.RENAME, 0.3
                ),
                UniqueFileNameGenerator::new,
                8,
                0,
                20,
                100
        );
    }

    @Test
    public void testParallelRenameSameFile() throws IOException, ExecutionException, InterruptedException {
        // renaming a single file in parallel. only one rename must win.

        final var threadCount = 8;
        final var rounds = 20;

        try (var executor = Executors.newFixedThreadPool(threadCount)) {
            for (int r = 0; r < rounds; r++) {
                final Map<String, byte[]> directoryContent;
                final var fileDir = createTempDir("parallel_rename_" + r);
                try (var dir = getDirectory(fileDir)) {
                    final var oldName = "name_1";
                    final var content = randomFileOfSize(dir, oldName, 10, 100);

                    final var cb = new CyclicBarrier(threadCount);

                    final var futures = new ArrayList<Future<String>>();
                    for (int i = 0; i < threadCount; i++) {
                        final var tid = i;
                        futures.add(executor.submit(() -> {
                            try {
                                final var newName = "new_name_" + tid;
                                cb.await();
                                dir.rename(oldName, newName);
                                return newName;
                            } catch (NoSuchFileException e) {
                                // that's fine
                                return null;
                            } catch (InterruptedException | BrokenBarrierException | IOException e) {
                                throw new RuntimeException(e);
                            }
                        }));
                    }

                    final var successfulRenames = new ArrayList<String>();
                    for (var future : futures) {
                        final var result = future.get();
                        if (result != null) {
                            successfulRenames.add(result);
                        }
                    }

                    assertEquals(1, successfulRenames.size());

                    final var newName = successfulRenames.getFirst();
                    directoryContent = Map.of(newName, content);

                    validateDirectoryContent(dir, directoryContent);
                }

                try (var dir = getDirectory(fileDir)) {
                    validateDirectoryContent(dir, directoryContent);
                }
            }
        }
    }

    @Test
    public void testParallelRenameIntoSameFile() throws IOException, ExecutionException, InterruptedException {
        // renaming multiple files with the same new name. only one rename must win.

        final var threadCount = 8;
        final var rounds = 20;

        try (var executor = Executors.newFixedThreadPool(threadCount)) {
            for (int r = 0; r < rounds; r++) {
                final Map<String, byte[]> directoryContent = new HashMap<>();
                final var fileDir = createTempDir("parallel_rename_" + r);
                final var newName = "new_name_1";

                try (var dir = getDirectory(fileDir)) {
                    for (int t = 0; t < threadCount; t++) {
                        final var oldName = "name_" + t;
                        final var content = randomFileOfSize(dir, oldName, 10, 100);
                        directoryContent.put(oldName, content);
                    }

                    final var cb = new CyclicBarrier(threadCount);

                    final var futures = new ArrayList<Future<String>>();
                    for (int i = 0; i < threadCount; i++) {
                        final var tid = i;
                        futures.add(executor.submit(() -> {
                            try {
                                final var oldName = "name_" + tid;
                                cb.await();
                                dir.rename(oldName, newName);
                                return oldName;
                            } catch (NoSuchFileException | FileAlreadyExistsException e) {
                                // that's fine
                                return null;
                            } catch (InterruptedException | BrokenBarrierException | IOException e) {
                                throw new RuntimeException(e);
                            }
                        }));
                    }

                    final var successfulRenames = new ArrayList<String>();
                    for (var future : futures) {
                        final var result = future.get();
                        if (result != null) {
                            successfulRenames.add(result);
                        }
                    }

                    assertEquals(1, successfulRenames.size());

                    final var oldName = successfulRenames.getFirst();
                    final var oldContent = directoryContent.remove(oldName);
                    directoryContent.put(newName, oldContent);

                    validateDirectoryContent(dir, directoryContent);
                }

                try (var dir = getDirectory(fileDir)) {
                    validateDirectoryContent(dir, directoryContent);
                }
            }
        }
    }

    @Test
    public void testParallelSameRenames() throws IOException, ExecutionException, InterruptedException {
        // executing same rename operations, but in parallel

        final var threadCount = 8;
        final var rounds = 20;

        try (var executor = Executors.newFixedThreadPool(threadCount)) {
            for (int r = 0; r < rounds; r++) {
                final Map<String, byte[]> directoryContent = new HashMap<>();
                final var fileDir = createTempDir("parallel_rename_" + r);
                final var oldName = "name_1";
                final var newName = "new_name_1";

                try (var dir = getDirectory(fileDir)) {
                    final var content = randomFileOfSize(dir, oldName, 10, 100);
                    directoryContent.put(oldName, content);

                    final var cb = new CyclicBarrier(threadCount);

                    final var futures = new ArrayList<Future<Boolean>>();
                    for (int i = 0; i < threadCount; i++) {
                        futures.add(executor.submit(() -> {
                            try {
                                cb.await();
                                dir.rename(oldName, newName);
                                return true;
                            } catch (NoSuchFileException | FileAlreadyExistsException e) {
                                // that's fine
                                return false;
                            } catch (InterruptedException | BrokenBarrierException | IOException e) {
                                throw new RuntimeException(e);
                            }
                        }));
                    }

                    var successfulRenames = 0;
                    for (var future : futures) {
                        final var result = future.get();
                        if (result) {
                            successfulRenames++;
                        }
                    }

                    assertEquals(1, successfulRenames);

                    final var oldContent = directoryContent.remove(oldName);
                    directoryContent.put(newName, oldContent);

                    validateDirectoryContent(dir, directoryContent);
                }

                try (var dir = getDirectory(fileDir)) {
                    validateDirectoryContent(dir, directoryContent);
                }
            }
        }
    }

    @Test
    public void testParallelRenameAndDelete() throws IOException, ExecutionException, InterruptedException {

        final var deletingThreads = 1;
        final var renamingThreads = 1;
        final var rounds = 20;

        try (var executor = Executors.newFixedThreadPool(deletingThreads + renamingThreads)) {
            for (int r = 0; r < rounds; r++) {
                final Map<String, byte[]> directoryContent = new HashMap<>();
                final var fileDir = createTempDir("delete_remove_test" + r);
                final var oldName = "name_1";
                final var newName = "new_name_1";

                try (var dir = getDirectory(fileDir)) {
                    final var content = randomFileOfSize(dir, oldName, 10, 100);
                    directoryContent.put(oldName, content);

                    final var cb = new CyclicBarrier(deletingThreads + renamingThreads);

                    final var futures = new ArrayList<Future<String>>();
                    for (int dt = 0; dt < deletingThreads; dt++) {
                        futures.add(executor.submit(() -> {
                            try {
                                cb.await();
                                dir.deleteFile(oldName);
                                return oldName;
                            } catch (NoSuchFileException e) {
                                return null;
                            }
                        }));
                    }

                    for (int rt = 0; rt < renamingThreads; rt++) {
                        futures.add(executor.submit(() -> {
                            try {
                                cb.await();
                                dir.rename(oldName, newName);
                                return newName;
                            } catch (NoSuchFileException e) {
                                return null;
                            }
                        }));
                    }

                    final var successfulOps = new ArrayList<String>();
                    for (var future : futures) {
                        final var result = future.get();
                        if (result != null) {
                            successfulOps.add(result);
                        }
                    }

                    assertEquals(1, successfulOps.size());

                    switch (successfulOps.getFirst()) {
                        case oldName -> directoryContent.remove(oldName);
                        case newName -> directoryContent.put(newName, directoryContent.remove(oldName));
                        default -> fail("");
                    }

                    validateDirectoryContent(dir, directoryContent);
                }

                try (var dir = getDirectory(fileDir)) {
                    validateDirectoryContent(dir, directoryContent);
                }
            }
        }
    }

    private <RESULT> void runMultiThreadedTest(
            int numOfThreads,
            int numOfRounds,
            int numOfIterationsPerThread,
            int numOfInitialFiles
    ) throws IOException {
        try (var executor = Executors.newFixedThreadPool(numOfThreads)) {
            for (int r = 0; r < numOfRounds; r++) {
                final var fileContentRegistry = new ConcurrentHashMap<String, byte[]>();
                final var fileDir = createTempDir(name.getMethodName() + "_" + r);

                try (var dir = getDirectory(fileDir)) {
                    for (int i = 0; i < numOfInitialFiles; i++) {
                        final var fileName = "initial_" + i;
                        final var content = randomFileOfSize(dir, fileName, 10, 100);
                        fileContentRegistry.put(fileName, content);
                    }

                    final var cb = new CyclicBarrier(numOfThreads);
                    final var futures = new ArrayList<Future<RESULT>>();
                }
            }
        }
    }

//    interface DirectoryWork {
//        void run(
//                Map<String, byte[]> directoryContent,
//                int threadId,
//                int roundId,
//                int
//        ) throws IOException;
//    }

    private void runMultiThreadedRandomTest(
            String dirName,
            Map<DirOperation, Double> operationsDistribution,
            Supplier<FileNameGenerator> fileNameGeneratorSup,
            int numOfThreads,
            int numOfInitialFiles,
            int numOfRounds,
            int numOfIterationsPerThread
    ) throws IOException, ExecutionException, InterruptedException {

        try (var executor = Executors.newFixedThreadPool(numOfThreads)) {
            for (int r = 0; r < numOfRounds; r++) {
                final var roundIndex = r;
                final var fileNameGenerator = fileNameGeneratorSup.get();
                final var fileContentRegistry = new ConcurrentHashMap<String, byte[]>();
                final var fileDir = createTempDir(dirName + "_" + r);

                try (var dir = getDirectory(fileDir)) {

                    for (int i = 0; i < numOfInitialFiles; i++) {
                        final var fileName = "initial_" + i;
                        final var content = randomFileOfSize(dir, fileName, 10, 100);
                        fileContentRegistry.put(fileName, content);
                    }

                    final var cb = new CyclicBarrier(numOfThreads);
                    final var work = IntStream.range(0, numOfThreads)
                            .mapToObj(i ->
                                    executor.submit(() -> {
                                        try {
                                            cb.await();
                                            runRandomWork(dir, roundIndex, i + 1, numOfIterationsPerThread, fileContentRegistry, operationsDistribution, fileNameGenerator);
                                        } catch (Exception e) {
                                            log.error("Failed to run random work", e);
                                            throw new RuntimeException(e);
                                        }
                                    })
                            )
                            .toList();

                    for (Future<?> future : work) {
                        future.get();
                    }

                    validateDirectoryContent(dir, fileContentRegistry);
                }

                // validate it once again after re-open
                try (var dir = getDirectory(fileDir)) {
                    validateDirectoryContent(dir, fileContentRegistry);
                }

            }
        }
    }

    private static void runRandomWork(
            Directory dir,
            int roundIdx,
            int threadIdx,
            int iterations,
            Map<String, byte[]> fileContentRegistry,
            Map<DirOperation, Double> opDistribution,
            FileNameGenerator fileNameGenerator
    ) throws IOException {

        for (int i = 0; i < iterations; i++) {
            final var it = i;

            // choose an operation based on opDistribution probabilities
            switch (randomValueFromDistribution(opDistribution)) {
                case CREATE -> {
                    try {
                        final var fileName = fileNameGenerator.peekName(roundIdx, threadIdx, it);
                        final var content = randomFileOfSize(dir, fileName, 10, 100);
                        fileNameGenerator.nameHasBeenUsed(fileName);
                        fileContentRegistry.put(fileName, content);
                    } catch (FileAlreadyExistsException e) {
                        // this is okay, ignoring it
                    }
                }
                case RENAME -> {
                    retryWhileException(e -> e instanceof NoSuchFileException || e instanceof FileAlreadyExistsException, () -> {
                        final var oldName = randomValueFromCollection(fileContentRegistry.keySet());

                        if (oldName != null) {
                            final var newName = fileNameGenerator.peekName(roundIdx, threadIdx, it);
                            dir.rename(oldName, newName);
                            final var oldContent = fileContentRegistry.remove(oldName);
                            assertNotNull("Empty content for new file " + oldName, oldContent);
                            fileNameGenerator.nameHasBeenUsed(newName);
                            fileContentRegistry.put(newName, oldContent);
                        }
                    });
                }
                case DELETE -> retryWhileException(e -> e instanceof NoSuchFileException, () -> {
                    final var fileName = randomValueFromCollection(fileContentRegistry.keySet());
                    if (fileName != null) {
                        dir.deleteFile(fileName);
                        fileContentRegistry.remove(fileName);
                    }
                });
            }
        }
    }

    private static void retryWhileException(Predicate<Exception> retryOn, IORunnable runnable) throws IOException {
        boolean retry = true;
        while (retry) {
            retry = false;
            try {
                runnable.run();
            } catch (Exception e) {
                if (retryOn.test(e)) {
                    retry = true;
                } else {
                    throw e;
                }
            }
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
            final var pageContentSize = ((XodusNonXodusDirectory) dir).getPageSize() - Long.BYTES;

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

    private static void validateDirectoryContent(
            Directory dir,
            Map<String, byte[]> expectedContent
    ) throws IOException {
        final var fileNames = Set.of(dir.listAll());

        assertEquals(
                expectedContent.keySet().stream().sorted().toList(),
                fileNames.stream().sorted().toList()
        );

        for (String fileName : expectedContent.keySet()) {
            final var expectedBytes = expectedContent.get(fileName);
            try (var i = dir.openInput(fileName, newIOContext(random()))) {

                assertEquals(expectedBytes.length, i.length());

                final var actualBytes = new byte[expectedBytes.length];
                i.readBytes(actualBytes, 0, actualBytes.length);

                assertArrayEquals(expectedBytes, actualBytes);
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

    private interface FileNameGenerator {
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
            final var oldName = namesPool.remove();

            // looks like this check breaks rename tests, and this scenario is valid.
//            if (!name.equals(oldName)) {
//                throw new IllegalArgumentException("This generator is being used in a not-correct way, names mismatch: " + oldName + " vs. " + name);
//            }
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
        private final Set<String> usedNames = Collections.newSetFromMap(new ConcurrentHashMap<>());

        @Override
        public String peekName(int roundIdx, int threadIdx, int iterationIdx) {
            return "file_" + roundIdx + "_" + threadIdx + "_" + iterationIdx;
        }

        @Override
        public void nameHasBeenUsed(String name) {
        }
    }

    @Test
    public void testMMapDir() throws IOException {
        final var fileDir = createTempDir();
        try (var dir = getDirectory(fileDir)) {
            final var bytes1 = RandomUtils.nextBytes(32 * 1024);
            final var bytes2 = RandomUtils.nextBytes(32 * 1024);

            try (var out = dir.createOutput("file1", newIOContext(random()))) {
                out.writeBytes(bytes1, bytes1.length);

                try (var in1 = dir.openInput("file1", newIOContext(random()))) {
                    final var bytes = new byte[((int) in1.length())];
                    in1.readBytes(bytes, 0, bytes.length);
                    assertArrayEquals(bytes1, bytes);
                }

                out.writeBytes(bytes2, bytes2.length);

                try (var in2 = dir.openInput("file1", newIOContext(random()))) {
                    final var bytes =  new byte[((int) in2.length())];
                    in2.readBytes(bytes, 0, bytes.length);
                    assertArrayEquals(ArrayUtils.addAll(bytes1, bytes2), bytes);
                }
            }

        }
    }
}
