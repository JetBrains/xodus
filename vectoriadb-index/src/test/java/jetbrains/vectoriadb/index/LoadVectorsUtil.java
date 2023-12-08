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
package jetbrains.vectoriadb.index;

import jetbrains.vectoriadb.index.siftbench.SiftBenchUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;

import java.io.IOException;
import java.nio.file.Path;

public class LoadVectorsUtil {
    public static int SIFT_VECTOR_DIMENSIONS = 128;
    public static int GIST_VECTOR_DIMENSIONS = 960;

    public static float[][] loadSift10KVectors() throws IOException {
        return loadVectors("siftsmall", SIFT_VECTOR_DIMENSIONS);
    }

    @SuppressWarnings("unused")
    public static float[][] loadSift1MVectors() throws IOException {
        return loadVectors("sift", SIFT_VECTOR_DIMENSIONS);
    }

    public static float[][] loadGist1MVectors() throws IOException {
        return loadVectors("gist", GIST_VECTOR_DIMENSIONS);
    }

    private static float[][] loadVectors(@NotNull String name, int vectorDimensions) throws IOException {
        var buildDir = System.getProperty("exodus.tests.buildDirectory");
        if (buildDir == null) {
            Assert.fail("exodus.tests.buildDirectory is not set !!!");
        }

        var archive = name + ".tar.gz";
        SiftBenchUtils.downloadSiftBenchmark(archive, buildDir);

        var dir = SiftBenchUtils.extractDataSet(Path.of(buildDir, archive), Path.of(buildDir, name));

        var filesDir = dir.resolve(name);

        var baseName = name + "_base.fvecs";
        var basePath = filesDir.resolve(baseName);

        System.out.println("Reading data vectors...");

        var vectors = SiftBenchUtils.readFVectors(basePath, vectorDimensions);

        System.out.printf("%d data vectors loaded with dimension %d%n", vectors.length, vectorDimensions);

        return vectors;
    }
}
