import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

dependencies {
    implementation(libs.commons.rng.simple)
    implementation(libs.commons.rng.sampling)

    implementation(libs.commons.net)
    implementation(libs.commons.compress)
    implementation(libs.commons.lang)
    implementation(libs.jcTools.core)
    implementation(libs.errorprone.annotations)

    annotationProcessor("org.openjdk.jmh:jmh-generator-annprocess:1.36")
    implementation("org.openjdk.jmh:jmh-core:1.36")
    testImplementation("org.apache.commons:commons-math3:3.6.1")
}

configurations {
    create("benchDependencies")
}

dependencies {
    "benchDependencies"("org.slf4j:slf4j-simple:2.0.7")
}

tasks {
    named("compileKotlin", KotlinCompile::class) {
        kotlinOptions {
            allWarningsAsErrors = true
        }
    }

    register<JavaExec>("runL2DistanceBench") {
        group = "application"
        mainClass = "jetbrains.exodus.diskann.bench.L2DistanceBench"
        classpath = sourceSets["main"].runtimeClasspath + configurations["benchDependencies"]
        jvmArgs = listOf(
            "-server",
            "-Xmx16g",
            "-XX:MaxDirectMemorySize=82g",
            "-XX:+HeapDumpOnOutOfMemoryError",
            "--add-modules",
            "jdk.incubator.vector",
            "-Djava.awt.headless=true",
            "--enable-preview"
        )
        systemProperties = mapOf(
            "bench.path" to (project.findProperty("bench.path"))
        )

        javaLauncher.set(rootProject.javaToolchains.launcherFor {
            languageVersion.set(JavaLanguageVersion.of(20))
        })
    }

    register<JavaExec>("runSift1MBench") {
        group = "application"
        mainClass = "jetbrains.vectoriadb.index.bench.SIFT1MBench"
        classpath = sourceSets["main"].runtimeClasspath + configurations["benchDependencies"]
        jvmArgs = listOf(
            "-server",
            "-Xmx16g",
            "-XX:MaxDirectMemorySize=110g",
            "-XX:+HeapDumpOnOutOfMemoryError",
            "--add-modules",
            "jdk.incubator.vector",
            "-Djava.awt.headless=true",
            "--enable-preview"
        )
        systemProperties = mapOf(
            "bench.path" to (project.findProperty("bench.path"))
        )


        javaLauncher.set(rootProject.javaToolchains.launcherFor {
            languageVersion.set(JavaLanguageVersion.of(20))
        })
    }

    register<JavaExec>("runBigANNBench") {
        group = "application"
        mainClass = "jetbrains.vectoriadb.index.bench.RunBigANNBench"
        classpath = sourceSets["main"].runtimeClasspath + configurations["benchDependencies"]
        jvmArgs = listOf(
            "-server",
            "-Xmx4096m",
            "-XX:MaxDirectMemorySize=125g",
            "-XX:+HeapDumpOnOutOfMemoryError",
            "--add-modules",
            "jdk.incubator.vector",
            "-Djava.awt.headless=true",
            "--enable-preview"
        )
        systemProperties = mapOf(
            "bench.path" to (project.findProperty("bench.path")),
            "m1-bench.path" to (project.findProperty("m1-bench.path"))
        )
    }

    register<JavaExec>("prepareRandomVectorBench") {
        group = "application"
        mainClass = "jetbrains.exodus.diskann.bench.PrepareRandomVectorBench"
        classpath = sourceSets["main"].runtimeClasspath + configurations["benchDependencies"]
        jvmArgs = listOf(
            "-server",
            "-Xmx16g",
            "-XX:MaxDirectMemorySize=82g",
            "-XX:+HeapDumpOnOutOfMemoryError",
            "--add-modules",
            "jdk.incubator.vector",
            "-Djava.awt.headless=true",
            "--enable-preview"
        )
    }

    register<JavaExec>("runRandomVectorBench") {
        group = "application"
        mainClass = "jetbrains.exodus.diskann.bench.RunRandomVectorBench"
        classpath = sourceSets["main"].runtimeClasspath + configurations["benchDependencies"]
        jvmArgs = listOf(
            "-server",
            "-Xmx16g",
            "-XX:MaxDirectMemorySize=82g",
            "-XX:+HeapDumpOnOutOfMemoryError",
            "--add-modules",
            "jdk.incubator.vector",
            "-Djava.awt.headless=true",
            "--enable-preview"
        )
    }

    register<JavaExec>("prepareBigANNBench") {
        group = "application"
        mainClass = "jetbrains.vectoriadb.index.bench.PrepareBigANNBench"
        classpath = sourceSets["main"].runtimeClasspath + configurations["benchDependencies"]
        jvmArgs = listOf(
            "-server",
            "-Xmx16g",
            "-XX:MaxDirectMemorySize=82g",
            "-XX:+HeapDumpOnOutOfMemoryError",
            "--add-modules",
            "jdk.incubator.vector",
            "-Djava.awt.headless=true",
            "--enable-preview"
        )
        systemProperties = mapOf(
            "bench.path" to (project.findProperty("bench.path"))
        )
    }

    register<JavaExec>("generateGroundTruthBigANNBench") {
        group = "application"
        mainClass = "jetbrains.vectoriadb.index.bench.GenerateGroundTruthBigANNBench"
        classpath = sourceSets["main"].runtimeClasspath + configurations["benchDependencies"]
        jvmArgs = listOf(
            "-server",
            "-Xmx92g",
            "-XX:+HeapDumpOnOutOfMemoryError",
            "--add-modules",
            "jdk.incubator.vector",
            "-Djava.awt.headless=true",
            "--enable-preview"
        )
        systemProperties = mapOf(
            "bench.path" to (project.findProperty("bench.path"))
        )
    }

    register<JavaExec>("runGist1MBench") {
        group = "application"
        mainClass = "jetbrains.exodus.diskann.bench.GIST1MBench"
        classpath = sourceSets["main"].runtimeClasspath + configurations["benchDependencies"]
        jvmArgs = listOf(
            "-server",
            "-Xmx16g",
            "-XX:MaxDirectMemorySize=82g",
            "-XX:+HeapDumpOnOutOfMemoryError",
            "--add-modules",
            "jdk.incubator.vector",
            "-Djava.awt.headless=true",
            "--enable-preview"
        )
        systemProperties = mapOf(
            "bench.path" to (project.findProperty("bench.path"))
        )
    }
}