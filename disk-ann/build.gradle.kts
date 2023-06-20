import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

dependencies {
    implementation(libs.commons.rng.simple)
    implementation(libs.commons.rng.sampling)

    implementation(libs.commons.net)
    implementation(libs.commons.compress)
    implementation(libs.commons.lang)

    testImplementation(project(":xodus-utils-test"))
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
    register<JavaExec>("runSift1MBench") {
        group = "application"
        mainClass = "jetbrains.exodus.diskann.bench.SIFT1MBench"
        classpath = sourceSets["main"].runtimeClasspath + configurations["benchDependencies"]
        jvmArgs = listOf(
            "-server", "-Xms16g", "-Xmx16g", "-XX:+HeapDumpOnOutOfMemoryError",
            "--add-modules", "jdk.incubator.vector", "-Djava.awt.headless=true",
            "-XX:+AlwaysPreTouch", "-XX:+UseTransparentHugePages", "-XX:+TieredCompilation",
            "-XX:+PrintInlining",
            "-Djdk.incubator.vector.VECTOR_ACCESS_OOB_CHECK=0"
        )
        systemProperties = mapOf(
            "bench.path" to (project.findProperty("bench.path"))
        )


        javaLauncher.set(rootProject.javaToolchains.launcherFor { languageVersion.set(JavaLanguageVersion.of(19)) })
    }

    register<JavaExec>("runGist1MBench") {
        group = "application"
        mainClass = "jetbrains.exodus.diskann.bench.GIST1MBench"
        classpath = sourceSets["main"].runtimeClasspath + configurations["benchDependencies"]
        jvmArgs = listOf(
            "-server", "-Xms16g", "-Xmx16g", "-XX:+HeapDumpOnOutOfMemoryError",
            "--add-modules", "jdk.incubator.vector", "-Djava.awt.headless=true",
            "-XX:+AlwaysPreTouch", "-XX:+UseTransparentHugePages", "-XX:+TieredCompilation"
        )
        systemProperties = mapOf(
            "bench.path" to (project.findProperty("bench.path"))
        )


        javaLauncher.set(rootProject.javaToolchains.launcherFor { languageVersion.set(JavaLanguageVersion.of(20)) })
    }
}