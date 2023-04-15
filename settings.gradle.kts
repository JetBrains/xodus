pluginManagement {
    plugins {
        id("org.jetbrains.kotlin.jvm") version("1.8.20")
        id("org.jetbrains.dokka") version("1.8.10")
        id("com.github.hierynomus.license") version("0.16.1")
        id("io.codearte.nexus-staging") version("0.30.0")
    }
    repositories {
        maven(url = "https://cache-redirector.jetbrains.com/plugins.gradle.org/m2")
        maven(url = "https://cache-redirector.jetbrains.com/repo1.maven.org/maven2")
    }
}

dependencyResolutionManagement {
    versionCatalogs {
        create("libs") {
            version("slf4j", "2.0.7")
            library("slf4j-api", "org.slf4j", "slf4j-api").versionRef("slf4j")
            library("slf4j-simple", "org.slf4j", "slf4j-simple").versionRef("slf4j")
            version("jetbrains-annotations", "24.0.0")
            library("jetbrains-annotations", "org.jetbrains", "annotations").versionRef("jetbrains-annotations")
            version("jcTools", "4.0.1")
            library("jcTools-core", "org.jctools", "jctools-core").versionRef("jcTools")
            version("junit", "4.13.2")
            library("junit", "junit", "junit").versionRef("junit")
            version("commons-compress", "1.22")
            library("commons-compress", "org.apache.commons", "commons-compress").versionRef("commons-compress")
            version("kotlin", "1.8.20")
            library("kotlin-stdlib", "org.jetbrains.kotlin", "kotlin-stdlib").versionRef("kotlin")
            library("kotlin-stdlib-jdk8", "org.jetbrains.kotlin", "kotlin-stdlib-jdk8").versionRef("kotlin")
            version("kotlin-logging", "3.0.5")
            library("kotlin-logging", "io.github.microutils", "kotlin-logging").versionRef("kotlin-logging")
            version("kotlin-lang", "1.8")
            version("commons-io", "2.11.0")
            library("commons-io", "commons-io", "commons-io").versionRef("commons-io")
            version("lz4", "1.8.0")
            library("lz4", "org.lz4", "lz4-java").versionRef("lz4")
            version("bouncyCastle", "1.70")
            library("bouncyCastle", "org.bouncycastle", "bcprov-jdk15on").versionRef("bouncyCastle")
            version("lucene", "8.10.0")
            library("lucene-core", "org.apache.lucene", "lucene-core").versionRef("lucene")
            library("lucene-analyzers-common", "org.apache.lucene", "lucene-analyzers-common").versionRef("lucene")
            library("lucene-queries", "org.apache.lucene", "lucene-queries").versionRef("lucene")
            library("lucene-queryparser", "org.apache.lucene", "lucene-queryparser").versionRef("lucene")
            library("lucene-test-framework", "org.apache.lucene", "lucene-test-framework").versionRef("lucene")
        }
    }
}

rootProject.name = "xodus"

include("utils")
project(":utils").name = "xodus-utils"

include("openAPI")
project(":openAPI").name = "xodus-openAPI"

include("compress")
project(":compress").name = "xodus-compress"

include("crypto")
project(":crypto").name = "xodus-crypto"

include("environment")
project(":environment").name = "xodus-environment"

include("entity-store")
project(":entity-store").name = "xodus-entity-store"

include("query")
project(":query").name = "xodus-query"

include("benchmarks")
project(":benchmarks").name = "xodus-benchmarks"

include("samples")
project(":samples").name = "xodus-samples"

include("tools")
project(":tools").name = "xodus-tools"

include("environment-crash-tests")
project(":environment-crash-tests").name = "xodus-environment-crash-tests"

include("lucene-directory-v2")
project(":lucene-directory-v2").name = "xodus-lucene-directory-v2"

