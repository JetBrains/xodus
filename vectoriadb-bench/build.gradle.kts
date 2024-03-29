dependencies {
    implementation(libs.commons.net)
    implementation(project(":vectoriadb-java-client")) {
        exclude("io.grpc", "grpc-core")
    }
    implementation("io.milvus:milvus-sdk-java:2.3.3")
    implementation("org.slf4j:slf4j-simple:2.0.7")
}

tasks {
    register<JavaExec>("runSift1MBench") {
        group = "application"
        mainClass = "jetbrains.vectoriadb.bench.Sift1MBench"
        classpath = sourceSets["main"].runtimeClasspath
        jvmArgs = listOf(
                "--add-modules",
                "jdk.incubator.vector",
                "-Djava.awt.headless=true",
                "--enable-preview"
        )
        systemProperties = mapOf(
                "bench.path" to (project.findProperty("bench.path")),
                "vectoriadb.host" to (project.findProperty("vectoriadb.host")),
                "vectoriadb.port" to (project.findProperty("vectoriadb.port"))
        )
    }

    register<JavaExec>("loadBigANNMilvus") {
        group = "application"
        mainClass = "jetbrains.vectoriadb.bench.BigANN500LoaderMilvus"
        classpath = sourceSets["main"].runtimeClasspath
        jvmArgs = listOf(
                "--add-modules",
                "jdk.incubator.vector",
                "-Djava.awt.headless=true",
                "--enable-preview"
        )
        systemProperties = mapOf(
                "bench.path" to (project.findProperty("bench.path")),
        )
    }
}