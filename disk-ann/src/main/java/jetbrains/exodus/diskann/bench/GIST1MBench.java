package jetbrains.exodus.diskann.bench;

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
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
