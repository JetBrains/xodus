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
        benchPath, "gist", "gist.tar.gz", "gist_base.fvecs",
        "gist_query.fvecs",
        "gist_groundtruth.ivecs", 960
    )
}
