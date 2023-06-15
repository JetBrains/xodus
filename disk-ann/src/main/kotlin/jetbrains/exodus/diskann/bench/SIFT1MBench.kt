package jetbrains.exodus.diskann.bench

import java.nio.file.Path

fun main() {
    val benchPathStr = System.getProperty("bench.path")
    val benchPath = if (benchPathStr != null) {
        Path.of(benchPathStr)
    } else {
        Path.of(".")
    }

    BenchUtils.runSiftBenchmarks(
        benchPath, "sift", "sift.tar.gz", "sift_base.fvecs",
        "sift_query.fvecs",
        "sift_groundtruth.ivecs", 128
    )
}
