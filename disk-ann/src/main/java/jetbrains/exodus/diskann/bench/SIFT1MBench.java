package jetbrains.exodus.diskann.bench;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;

public class SIFT1MBench {
    public static void main(String[] args) {
        var benchPathStr = System.getProperty("bench.path");
        Path benchPath;

        benchPath = Path.of(Objects.requireNonNullElse(benchPathStr, "."));

        try {
            BenchUtils.runSiftBenchmarks(
                    benchPath, "sift", "sift.tar.gz", "sift_base.fvecs",
                    "sift_query.fvecs",
                    "sift_groundtruth.ivecs", 128
            );
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
