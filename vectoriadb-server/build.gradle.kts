import com.bmuschko.gradle.docker.tasks.container.DockerCreateContainer
import com.bmuschko.gradle.docker.tasks.container.DockerStartContainer
import com.bmuschko.gradle.docker.tasks.container.DockerStopContainer
import com.bmuschko.gradle.docker.tasks.image.DockerBuildImage
import org.jetbrains.kotlin.gradle.internal.ensureParentDirsCreated
import org.jetbrains.kotlin.incremental.createDirectory

plugins {
    id("com.bmuschko.docker-remote-api")
}

dependencies {
    implementation(libs.spring.boot.starter) {
        exclude(group = "org.springframework.boot", module = "spring-boot-starter-logging")
    }

    implementation(libs.spring.boot.starter.log4j2)
    implementation(libs.log4j2.slf4j)

    implementation(libs.grpc.boot.starter)
    implementation(libs.grpc.java)
    implementation(libs.grpc.protobuf)
    implementation(libs.grpc.netty.shaded)

    implementation(libs.commons.io)

    implementation(project(":vectoriadb-interface"))
    implementation(project(":vectoriadb-index"))

    testImplementation(libs.grpc.test)
    testImplementation(libs.spring.boot.starter.test) {
        exclude(group = "org.springframework.boot", module = "spring-boot-starter-logging")
    }

    testImplementation(libs.commons.rng.simple)
    testImplementation(libs.commons.rng.sampling)

    testImplementation(libs.commons.net)
    testImplementation(libs.commons.compress)
    testImplementation(libs.commons.lang)
}

configurations {
    all {
        exclude(group = "org.springframework.boot", module = "spring-boot-starter-logging")
    }
}

tasks {
    test {
        systemProperty(
            "vectoriadb.log.dir",
            File(project.layout.buildDirectory.asFile.get(), "logs").absoluteFile
        )
    }
    val copyToLibs = register<Copy>("copyToLibs") {
        dependsOn(jar)
        from(project.configurations.getByName("runtimeClasspath"))
        into(project.file("build/libs"))
    }
    val buildDockerImage = register<DockerBuildImage>("buildDockerImage") {
        dependsOn(copyToLibs)
        inputDir = project.projectDir
        dockerFile = project.file("src/main/docker/Dockerfile")
        images.add("vectoriadb/vectoriadb-server:latest")
    }
    val createDockerContainer = register<DockerCreateContainer>("createDockerContainer") {
        dependsOn(buildDockerImage)
        targetImageId(buildDockerImage.get().imageId)

        val buildDir = project.layout.buildDirectory.asFile.get()
        val imageDir = File(buildDir, "vectoriadb-server")

        val config = File(imageDir, "config")
        val indexes = File(imageDir, "indexes")
        val logs = File(imageDir, "logs")

        config.ensureParentDirsCreated()
        config.createDirectory()

        indexes.ensureParentDirsCreated()
        indexes.createDirectory()

        logs.ensureParentDirsCreated()
        logs.createDirectory()

        volumes.add(config.absolutePath + ":/vectoriadb/config")
        volumes.add(indexes.absolutePath + ":/vectoriadb/indexes")
        volumes.add(logs.absolutePath + ":/vectoriadb/logs")

        hostConfig.portBindings.set(listOf("9090:9090"))
        hostConfig.autoRemove.set(true)
    }
    register<DockerStartContainer>("runServer") {
        dependsOn(createDockerContainer)
        targetContainerId(createDockerContainer.get().containerId)
    }
    register<DockerStopContainer>("stopServer") {
        targetContainerId(createDockerContainer.get().containerId)
    }
}
