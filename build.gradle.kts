import org.jetbrains.dokka.gradle.DokkaTask
import org.jetbrains.kotlin.gradle.plugin.extraProperties
import java.util.Calendar

plugins {
    kotlin("jvm")
    id("org.jetbrains.dokka")
    id("com.github.hierynomus.license")
    id("io.codearte.nexus-staging")
}

val xodusVersion: String? by project
val dailyBuild: String? by project
val mavenPublishUrl: String? by project
val mavenPublishUsername: String? by project
val mavenPublishPassword: String? by project
val signingKeyId: String? by project
val signingPassword: String? by project
val signingSecretKeyRingFile: String? by project

val isSnapshot: Boolean = xodusVersion?.endsWith("SNAPSHOT") ?: true
val isDailyBuild: Boolean = dailyBuild?.toBoolean() ?: false
val providedPublishUrl = mavenPublishUrl ?: ""
val providedPublishUsername = mavenPublishUsername ?: ""
val providedPublishPassword = mavenPublishPassword ?: ""
val providedSigningKeyId = signingKeyId ?: ""
val providedSigningPassword = signingPassword ?: ""
val providedSigningSecretKeyRingFile = signingSecretKeyRingFile ?: "../key.gpg"
val publishUrl: String =
    if (isDailyBuild) "https://packages.jetbrains.team/maven/p/xodus/xodus-daily" else providedPublishUrl

group = "org.jetbrains.xodus"
version = xodusVersion ?: version

fun shouldDeploy(project: Project): Boolean {
    return project.version.toString().isNotEmpty() && project.name !in listOf(
        "xodus-benchmarks", "xodus-samples",
        "xodus-environment-crash-tests"
    )
}

fun shouldApplyDokka(project: Project): Boolean {
    return project.version.toString().isNotEmpty() && project.name !in listOf(
        "xodus-benchmarks",
        "xodus-samples",
        "xodus-query",
        "xodus-environment-crash-tests"
    )
}

tasks.wrapper {
    gradleVersion = "8.4"
}

defaultTasks("assemble")

nexusStaging {
    username = providedPublishUsername
    password = providedPublishPassword
    delayBetweenRetriesInMillis = 30000
    stagingProfileId = "89ee7caa6631c4"
}

allprojects {
    repositories {
        maven { url = uri("https://cache-redirector.jetbrains.com/repo1.maven.org/maven2") }
        maven { url = uri("https://maven.pkg.jetbrains.space/public/p/kotlinx-html/maven") }
    }
}

subprojects {
    apply(plugin = "license")
    apply(plugin = "java")
    apply(plugin = "kotlin")
    apply(plugin = "signing")
    apply(plugin = "org.jetbrains.dokka")
    apply(plugin = "maven-publish")

    tasks.withType<JavaCompile> {
        options.encoding = "UTF-8"
    }

    group = rootProject.group
    version = rootProject.version

    val archivesBaseName = project.name
    setProperty("archivesBaseName", project.name)

    license {
        header = rootProject.file("license/copyright.ftl")
        strictCheck = true
        extraProperties["inceptionYear"] = 2010
        extraProperties["year"] = Calendar.getInstance().get(Calendar.YEAR)
        extraProperties["owner"] = "JetBrains s.r.o."
        include("**/*.kt")
        include("**/*.java")
        exclude("**/jetbrains/exodus/diskann/diskcache/**")
        exclude("**/vectoriadb/service/base/**")
        mapping(
            mapOf(
                "kt" to "SLASHSTAR_STYLE",
                "java" to "SLASHSTAR_STYLE"
            )
        )
    }

    dependencies {
        implementation(rootProject.libs.commons.compress)
        implementation(rootProject.libs.lz4)
        implementation(rootProject.libs.fastutil)
        implementation(rootProject.libs.jcTools.core)
        implementation(rootProject.libs.kotlin.stdlib)

        testImplementation(rootProject.libs.junit)

        if (name != "vectoriadb-server") {
            testImplementation(rootProject.libs.slf4j.simple)
        }
    }


    if (name !in listOf("benchmarks", "compress", "crypto", "openAPI", "samples", "utils")) {
        tasks.test {
            systemProperty("exodus.cipherId", "jetbrains.exodus.crypto.streamciphers.JBChaChaStreamCipherProvider")
            systemProperty("exodus.cipherKey", "000102030405060708090a0b0c0d0e0f000102030405060708090a0b0c0d0e0f")
            systemProperty("exodus.cipherBasicIV", "314159262718281828")
            systemProperty("exodus.useVersion1Format", "false")
            systemProperty("exodus.entityStore.useIntForLocalId", "true")
        }

        dependencies {
            testImplementation(project(":xodus-crypto"))
        }
    }
    tasks.withType<JavaCompile> {
        options.encoding = "UTF-8"
        options.compilerArgs = listOf("--add-modules", "jdk.incubator.vector", "--enable-preview")
    }

    tasks.jar {
        manifest {
            attributes(
                "Implementation-Title" to archivesBaseName,
                "Implementation-Version" to version
            )
        }
    }

    tasks.test {
        systemProperty("exodus.tests.buildDirectory", project.layout.buildDirectory.asFile.get())
        minHeapSize = "1g"
        maxHeapSize = "1g"
        jvmArgs = listOf(
            "-ea",
            "-XX:+HeapDumpOnOutOfMemoryError",
            "--add-modules",
            "jdk.incubator.vector",
            "--enable-preview"
        )
    }

    tasks.javadoc {
        isFailOnError = false
        options.quiet()
        (options as CoreJavadocOptions).addStringOption("Xdoclint:none", "-quiet")
        (options as CoreJavadocOptions).addBooleanOption("-enable-preview", true)
        (options as CoreJavadocOptions).addStringOption("source", 21.toString())
    }



    dependencies {
        implementation(rootProject.libs.kotlin.stdlib)
        implementation(rootProject.libs.kotlin.logging)
    }

    tasks.compileKotlin {
        kotlinOptions {
            languageVersion = libs.versions.kotlin.lang.get()
            apiVersion = libs.versions.kotlin.lang.get()
        }
    }
    tasks.compileTestKotlin {
        kotlinOptions {
            languageVersion = libs.versions.kotlin.lang.get()
            apiVersion = libs.versions.kotlin.lang.get()
        }
    }

    java {
        withJavadocJar()
        withSourcesJar()

        toolchain {
            languageVersion.set(JavaLanguageVersion.of(21))
        }
    }

    if (shouldApplyDokka(this)) {
        tasks.named<DokkaTask>("dokkaJavadoc") {
            dokkaSourceSets {
                configureEach {
                    reportUndocumented.set(false)
                }
            }
        }
        tasks.named<Jar>("javadocJar") {
            dependsOn("dokkaJavadoc")
            from(tasks.named<DokkaTask>("dokkaJavadoc").get().outputDirectory)
            duplicatesStrategy = DuplicatesStrategy.EXCLUDE
        }
    }

    if (!isSnapshot && providedSigningKeyId.isNotEmpty()) {
        extra["signing.keyId"] = providedSigningKeyId
        extra["signing.password"] = providedSigningPassword
        extra["signing.secretKeyRingFile"] = providedSigningSecretKeyRingFile
    }

    afterEvaluate {
        if (shouldDeploy(this) && !(isDailyBuild && name == "xodus-tools")) {
            configure<PublishingExtension> {
                repositories {
                    maven {
                        url = uri(publishUrl)
                        credentials {
                            username = providedPublishUsername
                            password = providedPublishPassword
                        }
                    }
                }
                publications {
                    create<MavenPublication>("mavenJava") {
                        artifactId = project.name
                        groupId = project.group.toString()
                        version = project.version.toString()
                        from(components["java"])
                        pom {
                            name.set("Xodus")
                            description.set("Xodus is pure Java transactional schema-less embedded database")
                            packaging = "jar"
                            url.set("https://github.com/JetBrains/xodus")
                            scm {
                                url.set("https://github.com/JetBrains/xodus")
                                connection.set("scm:git:https://github.com/JetBrains/xodus.git")
                                developerConnection.set("scm:git:https://github.com/JetBrains/xodus.git")
                            }

                            licenses {
                                license {
                                    name.set("The Apache Software License, Version 2.0")
                                    url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                                    distribution.set("repo")
                                }
                            }

                            developers {
                                developer {
                                    id.set("JetBrains")
                                    name.set("JetBrains Team")
                                    organization.set("JetBrains s.r.o")
                                    organizationUrl.set("https://www.jetbrains.com")
                                }
                            }
                        }
                    }
                }
            }

            configure<SigningExtension> {
                isRequired = !isSnapshot && providedSigningKeyId.isNotEmpty()
                sign(the<PublishingExtension>().publications["mavenJava"])
            }
        }
    }
}
