import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import kotlin.io.path.absolute

plugins {
    id("com.google.devtools.ksp")
}

dependencies {
    implementation(libs.commons.rng.simple)
    implementation(libs.commons.rng.sampling)

    implementation(libs.commons.net)
    implementation(libs.commons.compress)

    testImplementation(project(":xodus-utils-test"))
}

configurations {
    create("benchDependencies")
}

dependencies {
    "benchDependencies"("org.slf4j:slf4j-simple:2.0.7")
}



ksp {
    arg("excludePaths", project.projectDir.toPath().resolve("src").resolve("test").absolute().toString())
}

tasks {
    named("compileKotlin", KotlinCompile::class) {
        kotlinOptions {
            allWarningsAsErrors = true
        }
    }
    register<JavaExec>("runSift1MBench") {
        group = "application"
        mainClass = "jetbrains.exodus.diskann.bench.SIFT1MBenchKt"
        classpath = sourceSets["main"].runtimeClasspath + configurations["benchDependencies"]
        jvmArgs = listOf(
            "-server", "-Xms16g", "-Xmx16g", "-XX:+HeapDumpOnOutOfMemoryError",
            "--add-modules", "jdk.incubator.vector", "-Djava.awt.headless=true"
        )
        systemProperties = mapOf(
            "bench.path" to (project.findProperty("bench.path"))
        )


        javaLauncher.set(rootProject.javaToolchains.launcherFor { languageVersion.set(JavaLanguageVersion.of(19)) })
    }

    register<JavaExec>("runGist1MBench") {
        group = "application"
        mainClass = "jetbrains.exodus.diskann.bench.GIST1MBenchKt"
        classpath = sourceSets["main"].runtimeClasspath + configurations["benchDependencies"]
        jvmArgs = listOf(
            "-server", "-Xms16g", "-Xmx16g", "-XX:+HeapDumpOnOutOfMemoryError",
            "--add-modules", "jdk.incubator.vector", "-Djava.awt.headless=true"
        )
        systemProperties = mapOf(
            "bench.path" to (project.findProperty("bench.path"))
        )


        javaLauncher.set(rootProject.javaToolchains.launcherFor { languageVersion.set(JavaLanguageVersion.of(19)) })
    }
}