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

    register<JavaExec>("downloadDataset") {
        group = "application"
        mainClass = "jetbrains.vectoriadb.index.bench.DownloadDataset"
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
            "benchPath" to (project.findProperty("benchPath")),
            "dataset" to (project.findProperty("dataset"))
        )

        javaLauncher.set(rootProject.javaToolchains.launcherFor {
            languageVersion.set(JavaLanguageVersion.of(21))
        })
    }

    register<JavaExec>("calculateGroundTruth") {
        group = "application"
        mainClass = "jetbrains.vectoriadb.index.bench.CalculateGroundTruth"
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
            "benchPath" to (project.findProperty("benchPath")),
            "dataset" to (project.findProperty("dataset")),
            "distance" to (project.findProperty("distance")),
            "neighbourCount" to (project.findProperty("neighbourCount"))
        )

        javaLauncher.set(rootProject.javaToolchains.launcherFor {
            languageVersion.set(JavaLanguageVersion.of(21))
        })
    }

    register<JavaExec>("buildIndex") {
        group = "application"
        mainClass = "jetbrains.vectoriadb.index.bench.BuildIndex"
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
            "benchPath" to (project.findProperty("benchPath")),
            "dataset" to (project.findProperty("dataset")),
            "distance" to (project.findProperty("distance")),
            "indexName" to (project.findProperty("indexName")),
            "graphPartitionMemoryConsumptionGb" to (project.findProperty("graphPartitionMemoryConsumptionGb"))
        )

        javaLauncher.set(rootProject.javaToolchains.launcherFor {
            languageVersion.set(JavaLanguageVersion.of(21))
        })
    }

    register<JavaExec>("runBench") {
        group = "application"
        mainClass = "jetbrains.vectoriadb.index.bench.RunBench"
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
            "benchPath" to (project.findProperty("benchPath")),
            "dataset" to (project.findProperty("dataset")),
            "distance" to (project.findProperty("distance")),
            "indexName" to (project.findProperty("indexName")),
            "neighbourCount" to (project.findProperty("neighbourCount")),
            "cacheSizeGb" to (project.findProperty("cacheSizeGb")),
            "doWarmingUp" to (project.findProperty("doWarmingUp")),
            "repeatTimes" to (project.findProperty("repeatTimes")),
            "recallCount" to (project.findProperty("recallCount")),
        )

        javaLauncher.set(rootProject.javaToolchains.launcherFor {
            languageVersion.set(JavaLanguageVersion.of(21))
        })
    }

    register<JavaExec>("prepareAndRunBench") {
        group = "application"
        mainClass = "jetbrains.vectoriadb.index.bench.PrepareAndRunBench"
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
            "benchPath" to (project.findProperty("benchPath")),
            "dataset" to (project.findProperty("dataset")),
            "distance" to (project.findProperty("distance")),
            "indexName" to (project.findProperty("indexName")),
            "graphPartitionMemoryConsumptionGb" to (project.findProperty("graphPartitionMemoryConsumptionGb")),
            "neighbourCount" to (project.findProperty("neighbourCount")),
            "cacheSizeGb" to (project.findProperty("cacheSizeGb")),
            "doWarmingUp" to (project.findProperty("doWarmingUp")),
            "repeatTimes" to (project.findProperty("repeatTimes")),
            "recallCount" to (project.findProperty("recallCount")),
        )

        javaLauncher.set(rootProject.javaToolchains.launcherFor {
            languageVersion.set(JavaLanguageVersion.of(21))
        })
    }

    register<JavaExec>("runDistanceComputationBenchmark") {
        group = "application"
        mainClass = "jetbrains.vectoriadb.index.bench.DistanceComputationBenchmark"
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

        javaLauncher.set(rootProject.javaToolchains.launcherFor {
            languageVersion.set(JavaLanguageVersion.of(21))
        })
    }
}