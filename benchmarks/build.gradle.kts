val currentJmhVersion = "1.35"

buildscript {
    repositories {
        maven {
            url = uri("https://cache-redirector.jetbrains.com/plugins.gradle.org/m2")
        }
    }
    dependencies {
        classpath("org.apache.commons:commons-lang3:3.5")
    }
}

plugins {
    id("me.champeau.jmh")
}


dependencies {
    val jmh = configurations.getByName("jmh")
    jmh(project(":xodus-environment"))
    jmh(project(":xodus-entity-store"))
    jmh(project(":xodus-query"))
    jmh(project(":xodus-crypto"))
    jmh(project(":xodus-utils"))
    jmh("org.openjdk.jmh:jmh-core:$currentJmhVersion")
    jmh("org.openjdk.jmh:jmh-generator-annprocess:$currentJmhVersion")
    jmh(libs.junit)
}

jmh {
    excludes.set(listOf("dataStructures|util|query|crypto"))
    includes.set(listOf("env.*"))
    jmhVersion.set(currentJmhVersion)
    jvmArgs.set(listOf("-server", "-Xmx1g", "-Xms1g", "-XX:+HeapDumpOnOutOfMemoryError"))
    duplicateClassesStrategy.set(DuplicatesStrategy.WARN)
    resultFormat.set("JSON")
}

val deleteEmptyBenchmarkList = tasks.register<Delete>("deleteEmptyBenchmarkList") {
    delete("$buildDir/jmh-generated-classes/META-INF/BenchmarkList")
}


tasks.named("jmhCompileGeneratedClasses") {
    finalizedBy(deleteEmptyBenchmarkList)
}