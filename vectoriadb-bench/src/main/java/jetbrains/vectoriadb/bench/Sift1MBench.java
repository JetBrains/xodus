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
package jetbrains.vectoriadb.bench;

import jetbrains.vectoriadb.client.Distance;
import jetbrains.vectoriadb.client.IndexBuildStatusListener;
import jetbrains.vectoriadb.client.IndexState;
import jetbrains.vectoriadb.client.VectoriaDBClient;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public class Sift1MBench {
    public static void main(String[] args) {
        try {
            var benchPathStr = System.getProperty("bench.path");
            var benchPath = Path.of(Objects.requireNonNullElse(benchPathStr, "."));
            Files.createDirectories(benchPath);

            var rootDir = benchPath.resolve("sift1m");
            Files.createDirectories(rootDir);

            var siftArchiveName = "sift.tar.gz";
            var vectorDimensions = 128;

            System.out.println("Working directory: " + rootDir.toAbsolutePath());

            var siftArchivePath = BenchUtils.downloadBenchFile(rootDir, siftArchiveName);
            BenchUtils.extractTarGzArchive(rootDir, siftArchivePath);

            var siftDir = rootDir.resolve("sift");
            var siftDataName = "sift_base.fvecs";
            var vectors = BenchUtils.readFVectors(siftDir.resolve(siftDataName), vectorDimensions);

            var indexName = "sift1m";
            System.out.printf("%d data vectors loaded with dimension %d, building index %s...%n",
                    vectors.length, vectorDimensions, indexName);

            var vectoriaDBHost = System.getProperty("vectoriadb.host", "localhost");
            if (vectoriaDBHost.trim().isEmpty()) {
                vectoriaDBHost = "localhost";
            }

            var vectoriaDBPortProperty = System.getProperty("vectoriadb.port", "9090");
            if (vectoriaDBPortProperty.trim().isEmpty()) {
                vectoriaDBPortProperty = "9090";
            }

            var vectoriaDBPort = Integer.parseInt(vectoriaDBPortProperty);

            var client = new VectoriaDBClient(vectoriaDBHost, vectoriaDBPort);

            var ts1 = System.currentTimeMillis();
            client.createIndex(indexName, Distance.L2);
            var ts2 = System.currentTimeMillis();
            System.out.printf("Index %s created in %d ms, uploading vectors %n", indexName, ts2 - ts1);

            ts1 = System.currentTimeMillis();

            client.uploadVectors(indexName, vectors, (current, count) -> {
                if (current >= 0 && current < Integer.MAX_VALUE) {
                    if (current % 1_000 == 0) {
                        System.out.printf("%d vectors uploaded out of %d%n", current, count);
                    }
                } else if (current < 0) {
                    System.out.println("Waiting for confirmation from server side");
                } else {
                    System.out.println("Upload completed.");
                }
            });

            ts2 = System.currentTimeMillis();
            System.out.printf("%d vectors uploaded in %d ms, building index %n", vectors.length, ts2 - ts1);

            ts1 = System.currentTimeMillis();
            client.buildIndex(indexName);

            var stopPrintStatus = new AtomicBoolean();

            client.buildStatusAsync((name, phases) -> {
                printStatus(name, phases);
                return !stopPrintStatus.get();
            });

            while (true) {
                var indexState = client.indexState(indexName);
                if (indexState != IndexState.BUILDING && indexState != IndexState.BUILT &&
                        indexState != IndexState.IN_BUILD_QUEUE) {
                    throw new IllegalStateException("Unexpected index state: " + indexState);
                }

                if (indexState == IndexState.BUILT) {
                    break;
                }
            }

            ts2 = System.currentTimeMillis();
            System.out.printf("Index %s built in %d ms%n", indexName, ts2 - ts1);
            client.switchToSearchMode();

            var queryFileName = "sift_query.fvecs";

            System.out.println("Reading queries...");
            var queryFile = siftDir.resolve(queryFileName);
            var queryVectors = BenchUtils.readFVectors(queryFile, vectorDimensions);

            System.out.println(queryVectors.length + " queries are read");
            System.out.println("Reading ground truth...");

            var groundTruthFileName = "sift_groundtruth.ivecs";
            var groundTruthFile = siftDir.resolve(groundTruthFileName);
            var groundTruth = BenchUtils.readIVectors(groundTruthFile, 100);

            System.out.println("Ground truth is read, searching...");
            System.out.println("Warming up ...");

            for (int i = 0; i < 10; i++) {
                for (float[] vector : queryVectors) {
                    client.findNearestNeighbours(indexName, vector, 1);
                }
            }

            System.out.println("Benchmark ...");

            ts1 = System.nanoTime();
            var errorsCount = 0;
            for (var index = 0; index < queryVectors.length; index++) {
                var vector = queryVectors[index];

                var result = client.findNearestNeighbours(indexName, vector, 1);
                if (groundTruth[index][0] != result[0]) {
                    errorsCount++;
                }
            }
            ts2 = System.nanoTime();
            var errorPercentage = errorsCount * 100.0 / queryVectors.length;

            System.out.printf("Avg. query time : %d us, errors: %f%% %n",
                    (ts2 - ts1) / 1000 / queryVectors.length, errorPercentage);

        } catch (Exception e) {
            //noinspection CallToPrintStackTrace
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private static void printStatus(String indexName, List<IndexBuildStatusListener.Phase> phases) {
        if (indexName == null || phases.isEmpty()) {
            return;
        }

        StringBuilder builder = new StringBuilder();
        builder.append(indexName).append(" : ");

        int counter = 0;
        for (var phase : phases) {
            if (counter > 0) {
                builder.append(" -> ");
            }

            builder.append(phase.name());
            var parameters = phase.parameters();

            if (parameters.length > 0) {
                builder.append(" ");
            }

            for (int j = 0; j < parameters.length; j += 2) {
                builder.append("{");
                builder.append(parameters[j]);
                builder.append(":");
                builder.append(parameters[j + 1]);
                builder.append("}");

                if (j < parameters.length - 2) {
                    builder.append(", ");
                }
            }
            if (phase.progress() >= 0) {
                builder.append(" [").append(String.format("%.2f", phase.progress())).append("%]");
            }
            counter++;
        }

        System.out.println(builder);
    }
}
