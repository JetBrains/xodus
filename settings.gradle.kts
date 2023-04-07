pluginManagement {
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

include("vfs")
project(":vfs").name = "xodus-vfs"

include("lucene-directory")
project(":lucene-directory").name = "xodus-lucene-directory"

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

