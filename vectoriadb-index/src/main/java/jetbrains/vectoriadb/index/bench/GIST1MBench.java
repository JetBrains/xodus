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
package jetbrains.vectoriadb.index.bench;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;

public class GIST1MBench {
    public static void main(String[] args) {
        var benchPathStr = System.getProperty("bench.path");
        Path benchPath;

        benchPath = Path.of(Objects.requireNonNullElse(benchPathStr, "."));

        try {
            BenchUtils.runSiftBenchmarks(
                    benchPath, "gist", "gist.tar.gz", "gist_base.fvecs",
                    "gist_query.fvecs",
                    "gist_groundtruth.ivecs", 960
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
