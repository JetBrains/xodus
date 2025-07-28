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
import org.apache.lucene.util.IORunnable;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public abstract class XodusDirectoryBaseTest extends BaseDirectoryTestCase {

    private static final Logger log = LoggerFactory.getLogger(XodusDirectoryBaseTest.class);

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

    protected Directory getDirectoryOldVersion(Path path) throws IOException {

        return new XodusDirectory(
                Environments.newInstance(
                        path.toFile(),
                        getEnvironmentConfig()
                )
        );
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
            final var fileNames = Set.of(dir.listAll());

            assertEquals(fileNames, content.keySet());

            for (String fileName : content.keySet()) {
                final var expectedBytes = content.get(fileName);
                try (var i = dir.openInput(fileName, newIOContext(random()))) {

                    assertEquals(expectedBytes.length, i.length());

                    final var actualBytes = new byte[expectedBytes.length];
                    i.readBytes(actualBytes, 0, actualBytes.length);

                    assertArrayEquals(expectedBytes, actualBytes);
                }
            }
        }
    }

    private enum DirOperation {CREATE, RENAME, DELETE}


    @Test
    public void testParallelDelete() throws IOException, ExecutionException, InterruptedException {
        runMultiThreadingTest(
                Map.of(DirOperation.DELETE, 1.0),
                8,
                50,
                20,
                10
        );
    }

    @Test
    public void testParallelRename() throws IOException, ExecutionException, InterruptedException {
        runMultiThreadingTest(
                Map.of(DirOperation.RENAME, 1.0),
                8,
                50,
                20,
                10
        );
    }

    private void runMultiThreadingTest(
            Map<DirOperation, Double> operationsDistribution,
            int numOfThreads,
            int numOfInitialFiles,
            int numOfRounds,
            int numOfIterationsPerThread
    ) throws IOException, ExecutionException, InterruptedException {

        try (var executor = Executors.newFixedThreadPool(numOfThreads)) {
            for (int r = 0; r < numOfRounds; r++) {

                try (var dir = getDirectory(createTempDir("testBrokenDirCleanup_" + r))) {
                    final Set<String> names = Collections.newSetFromMap(new ConcurrentHashMap<>());
                    for (int i = 0; i < numOfInitialFiles; i++) {
                        final var fileName = "initial_" + i;
                        createFileLimitBytes(dir, fileName, 10, 100);
                        names.add(fileName);
                    }

                    final var cb = new CyclicBarrier(numOfThreads);
                    final var work = IntStream.range(0, numOfThreads)
                            .mapToObj(i ->
                                    executor.submit(() -> {
                                        try {
                                            cb.await();
                                            runRandomWork(dir, "" + i, numOfIterationsPerThread, names, operationsDistribution);
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

                    assertEquals(names.size(), dir.listAll().length);
                }
            }
        }
    }

    private static void runRandomWork(
            Directory dir,
            String nameSuffix,
            int iterations,
            Set<String> names,
            Map<DirOperation, Double> opDistribution
    ) throws IOException {

        for (int i = 0; i < iterations; i++) {
            // choose an operation based on opDistribution probabilities
            switch (randomValueFromDistribution(opDistribution)) {
                case CREATE -> {
                    final var fileName = "file_" + nameSuffix + "_" + i;
                    createFileLimitBytes(dir, fileName, 10, 100);
                    names.add(fileName);
                }
                case RENAME -> {
                    final var newName = "file_" + nameSuffix + "_" + i;
                    retryWhileException(e -> e instanceof FileNotFoundException, () -> {
                        final var oldName = randomValueFromCollection(names);

                        if (oldName != null) {
                            try {
                                dir.rename(oldName, newName);
                            } finally {
                                names.remove(oldName);
                                names.add(newName);
                            }
                        }
                    });
                }
                case DELETE -> retryWhileException(e -> e instanceof FileNotFoundException, () -> {
                    final var fileName = randomValueFromCollection(names);
                    if (fileName != null) {
                        try {
                            dir.deleteFile(fileName);
                        } finally {
                            names.remove(fileName);
                        }
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

    private static void createFile(
            Directory dir,
            String fileName,
            Function<String, byte[]> contentSupplier
    ) throws IOException {
        try (var out = dir.createOutput(fileName, newIOContext(random()))) {
            final var bytes = contentSupplier.apply(fileName);
            out.writeBytes(bytes, bytes.length);
        }
    }

    protected static void createFileLimitPages(
            Directory dir,
            String fileName,
            int numOfPages
    ) throws IOException {
        createFile(dir, fileName, fn -> {
            final var pageContentSize = ((XodusNonXodusDirectory) dir).getPageSize() - Long.BYTES;

            final var minContentSize = (numOfPages - 1) * pageContentSize + 1;
            final var maxContentSize = numOfPages * pageContentSize;

            final var length = RandomUtils.nextInt(minContentSize, maxContentSize + 1);

            return RandomUtils.nextBytes(length);
        });
    }

    private static void createFileLimitBytes(
            Directory dir,
            String fileName,
            int minContentSize,
            int maxContentSize
    ) throws IOException {
        createFile(
                dir, fileName,
                fn -> RandomUtils.nextBytes(RandomUtils.nextInt(minContentSize, maxContentSize + 1))
        );
    }

    private static <T> T randomValueFromCollection(Collection<T> col) {
        return col.stream().skip(RandomUtils.nextInt(0, col.size())).findFirst().orElse(null);
    }

    private static <T> T randomValueFromDistribution(Map<T, Double> distribution) {
        final var v = RandomUtils.nextDouble(0, 1);
        double sum = 0;
        for (var e : distribution.entrySet()) {
            sum += e.getValue();
            if (v < sum) {
                return e.getKey();
            }
        }
        throw new IllegalArgumentException("Distribution is invalid");
    }
}
