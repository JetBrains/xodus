dependencies {
    implementation(libs.commons.net)
    implementation(project(":vectoriadb-java-client"))
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
}