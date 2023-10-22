pluginManagement {
    plugins {
        id("org.jetbrains.kotlin.jvm") version ("1.9.20-RC")
        id("org.jetbrains.dokka") version ("1.8.10")
        id("com.github.hierynomus.license") version ("0.16.1")
        id("io.codearte.nexus-staging") version ("0.30.0")
        id("com.github.johnrengelman.shadow") version ("8.1.1")
        id("me.champeau.jmh") version ("0.7.1")
        id("com.google.protobuf") version ("0.9.4")
        id("org.springframework.boot") version ("3.1.4")
        id("org.jetbrains.gradle.plugin.idea-ext") version "1.1.7"
        id("org.gradle.toolchains.foojay-resolver-convention") version ("0.7.0")
    }
    repositories {
        maven(url = "https://cache-redirector.jetbrains.com/plugins.gradle.org/m2")
        maven(url = "https://cache-redirector.jetbrains.com/repo1.maven.org/maven2")
    }
}

dependencyResolutionManagement {
    versionCatalogs {
        create("libs") {
            version("kotlin-lang", "1.9")
            version("kotlin", "1.9.20-RC")

            version("kotlin-logging", "3.0.5")
            version("lz4", "1.8.0")
            version("slf4j", "2.0.7")
            version("jetbrains-annotations", "24.0.0")
            version("jcTools", "4.0.1")
            version("junit", "4.13.2")
            version("commons-compress", "1.22")
            version("commons-net", "3.9.0")
            version("bouncyCastle", "1.70")
            version("commons-io", "2.11.0")
            version("commons-lang", "3.12.0")
            version("lucene", "8.10.0")
            version("fastutil", "8.5.9")
            version("commons-rng", "1.5")
            version("caffeine", "3.1.8")
            version("errorprone", "2.21.1")
            version("grpc", "1.58.0")
            version("protobuf", "3.24.3")
            version("javax-annotation", "1.3.2")

            version("spring-boot", "3.1.4")
            version("grpc-boot-starter", "2.14.0.RELEASE")

            library("slf4j-api", "org.slf4j", "slf4j-api").versionRef("slf4j")
            library("slf4j-simple", "org.slf4j", "slf4j-simple").versionRef("slf4j")

            library("jetbrains-annotations", "org.jetbrains", "annotations").versionRef("jetbrains-annotations")

            library("jcTools-core", "org.jctools", "jctools-core").versionRef("jcTools")

            library("junit", "junit", "junit").versionRef("junit")

            library("commons-compress", "org.apache.commons", "commons-compress").versionRef("commons-compress")
            library("commons-net", "commons-net", "commons-net").versionRef("commons-net")
            library("commons-lang", "org.apache.commons", "commons-lang3").versionRef("commons-lang")

            library("kotlin-stdlib", "org.jetbrains.kotlin", "kotlin-stdlib").versionRef("kotlin")
            library("kotlin-stdlib-jdk8", "org.jetbrains.kotlin", "kotlin-stdlib-jdk8").versionRef("kotlin")

            library("kotlin-logging", "io.github.microutils", "kotlin-logging").versionRef("kotlin-logging")

            library("commons-io", "commons-io", "commons-io").versionRef("commons-io")
            library("lz4", "org.lz4", "lz4-java").versionRef("lz4")

            library("bouncyCastle", "org.bouncycastle", "bcprov-jdk15on").versionRef("bouncyCastle")

            library("lucene-core", "org.apache.lucene", "lucene-core").versionRef("lucene")
            library("lucene-analyzers-common", "org.apache.lucene", "lucene-analyzers-common").versionRef("lucene")
            library("lucene-queries", "org.apache.lucene", "lucene-queries").versionRef("lucene")
            library("lucene-queryparser", "org.apache.lucene", "lucene-queryparser").versionRef("lucene")
            library("lucene-test-framework", "org.apache.lucene", "lucene-test-framework").versionRef("lucene")

            library("fastutil", "it.unimi.dsi", "fastutil").versionRef("fastutil")

            library(
                "commons-rng-simple", "org.apache.commons",
                "commons-rng-simple"
            ).versionRef("commons-rng")
            library(
                "commons-rng-sampling", "org.apache.commons",
                "commons-rng-sampling"
            ).versionRef("commons-rng")

            library("caffeine", "com.github.ben-manes.caffeine", "caffeine").versionRef("caffeine")

            library(
                "errorprone-annotations",
                "com.google.errorprone",
                "error_prone_annotations"
            ).versionRef("errorprone")

            library("protobuf-protoc", "com.google.protobuf", "protoc").versionRef("protobuf")
            library("protobuf-java", "com.google.protobuf", "protobuf-java").versionRef("protobuf")

            library("grpc-java", "io.grpc", "protoc-gen-grpc-java").versionRef("grpc")
            library("grpc-stub", "io.grpc", "grpc-stub").versionRef("grpc")
            library("grpc-protobuf", "io.grpc", "grpc-protobuf").versionRef("grpc")

            library(
                "javax-annotation-api", "javax.annotation",
                "javax.annotation-api"
            ).versionRef("javax-annotation")

            library(
                "spring-boot-starter", "org.springframework.boot",
                "spring-boot-starter"
            ).versionRef("spring-boot")

            library(
                "grpc-boot-starter", "net.devh",
                "grpc-server-spring-boot-starter"
            ).versionRef("grpc-boot-starter")
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

include("environment-test")
project(":environment-test").name = "xodus-environment-test"

include("utils-test")
project(":utils-test").name = "xodus-utils-test"

include("vectoriadb-index")
project(":vectoriadb-index").name = "xodus-vectoriadb-index"

include("vectoriadb-server")
project(":vectoriadb-server").name = "xodus-vectoriadb-server"

include("vectoriadb-interface")
project(":vectoriadb-interface").name = "xodus-vectoriadb-interface"

include("vectoriadb-java-client")
project(":vectoriadb-java-client").name = "xodus-vectoriadb-java-client"
