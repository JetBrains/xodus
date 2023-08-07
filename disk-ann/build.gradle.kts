import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

dependencies {
    implementation(libs.commons.rng.simple)
    implementation(libs.commons.rng.sampling)

    implementation(libs.commons.net)
    implementation(libs.commons.compress)
    implementation(libs.commons.lang)
    implementation(libs.jcTools.core)

    annotationProcessor("org.openjdk.jmh:jmh-generator-annprocess:1.36")
    implementation("org.openjdk.jmh:jmh-core:1.36")

    testImplementation(project(":xodus-utils-test"))
    testImplementation("org.apache.commons:commons-math3:3.6.1")
}

configurations {
    create("benchDependencies")
}

dependencies {
    "benchDependencies"("org.slf4j:slf4j-simple:2.0.7")
}

val jdkHome: String? = findProperty("jdkHome") as String?
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
            "-XX:+HeapDumpOnOutOfMemoryError",
            "--add-modules",
            "jdk.incubator.vector",
            "-Djava.awt.headless=true",
            "--enable-preview"
        )
        systemProperties = mapOf(
            "bench.path" to (project.findProperty("bench.path"))
        )

        if (jdkHome != null) {
            executable = "$jdkHome/bin/java"
        } else {
            javaLauncher.set(rootProject.javaToolchains.launcherFor {
                languageVersion.set(JavaLanguageVersion.of(20))
            })
        }
    }

    register<JavaExec>("runSift1MBench") {
        group = "application"
        mainClass = "jetbrains.exodus.diskann.bench.SIFT1MBench"
        classpath = sourceSets["main"].runtimeClasspath + configurations["benchDependencies"]
        jvmArgs = listOf(
            "-server",
            "-Xmx16g",
            "-XX:+HeapDumpOnOutOfMemoryError",
            "--add-modules",
            "jdk.incubator.vector",
            "-Djava.awt.headless=true",
            "--enable-preview"
        )
        systemProperties = mapOf(
            "bench.path" to (project.findProperty("bench.path"))
        )

        if (jdkHome != null) {
            executable = "$jdkHome/bin/java"
        } else {
            javaLauncher.set(rootProject.javaToolchains.launcherFor {
                languageVersion.set(JavaLanguageVersion.of(20))
            })
        }
    }

    register<JavaExec>("runGist1MBench") {
        group = "application"
        mainClass = "jetbrains.exodus.diskann.bench.GIST1MBench"
        classpath = sourceSets["main"].runtimeClasspath + configurations["benchDependencies"]
        jvmArgs = listOf(
            "-server",
            "-Xmx16g",
            "-XX:+HeapDumpOnOutOfMemoryError",
            "--add-modules",
            "jdk.incubator.vector",
            "-Djava.awt.headless=true",
            "--enable-preview"
        )
        systemProperties = mapOf(
            "bench.path" to (project.findProperty("bench.path"))
        )


        if (jdkHome != null) {
            executable = "$jdkHome/bin/java"
        } else {
            javaLauncher.set(rootProject.javaToolchains.launcherFor {
                languageVersion.set(JavaLanguageVersion.of(20))
            })
        }
    }
}