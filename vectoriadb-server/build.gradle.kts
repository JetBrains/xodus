import com.bmuschko.gradle.docker.tasks.container.DockerCreateContainer
import com.bmuschko.gradle.docker.tasks.container.DockerRemoveContainer
import com.bmuschko.gradle.docker.tasks.image.DockerBuildImage
import java.io.BufferedReader
import java.io.IOException
import java.io.InputStreamReader
import java.nio.file.Files


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

    register<DockerBuildImage>("buildDockerImage") {
        dependsOn(copyToLibs)
        inputDir = project.projectDir
        dockerFile = project.file("src/main/docker/Dockerfile")
        images.add("vectoriadb/vectoriadb-server:latest")
    }

    val buildDockerImageDebug = register<DockerBuildImage>("buildDockerImageDebug") {
        dependsOn(copyToLibs)

        dockerFile = project.file("src/main/docker/Dockerfile-debug")
        inputDir = project.projectDir

//        val userId = fetchCurrentUserId()
//        val groupId = fetchCurrentUserGroupId()
//
//        if (userId >= 0) {
//            logger.info("Container will be built as $userId:$groupId")
//            buildArgs.set(mapOf("USER_ID" to userId.toString(),
//                    "GROUP_ID" to groupId.toString()))
//        } else {
//            logger.info("Container will be built as root")
//        }

        images.add("vectoriadb/vectoriadb-server-debug:latest")
    }


    val removeDockerContainerDebug = register<DockerRemoveContainer>("removeDockerContainerDebug") {
        targetContainerId("vectoriadb-server-debug")

        onError {
            val message = this.message

            if (message != null && message.contains("No such container")) {
                return@onError
            }
            throw RuntimeException(this)
        }
    }

    register<DockerCreateContainer>("createDockerContainerDebug") {
        dependsOn(buildDockerImageDebug)
        dependsOn(removeDockerContainerDebug)
        targetImageId(buildDockerImageDebug.get().imageId)

        containerName = "vectoriadb-server-debug"
//        val userId = fetchCurrentUserId()
//        val userGroup = fetchCurrentUserGroupId()
//
//        if (userId >= 0) {
//            logger.info("Container will be run as $userId:$userGroup")
//            user = "$userId:$userGroup"
//        }

        val containerDirectory = project.layout.buildDirectory.asFile.get()
        val imageDir = File(containerDirectory, "vectoriadb-server")

        val config = File(imageDir, "config")
        val indexes = File(imageDir, "indexes")
        val logs = File(imageDir, "logs")

        Files.createDirectories(config.toPath())
        Files.createDirectories(indexes.toPath())
        Files.createDirectories(logs.toPath())

        hostConfig.binds.set(mapOf(config.canonicalPath to "/vectoriadb/config",
                indexes.canonicalPath to "/vectoriadb/indexes", logs.canonicalPath to "/vectoriadb/logs"))
        hostConfig.portBindings.set(listOf("9090:9090", "5005:5005"))
    }
}

fun fetchCurrentUserId(): Int {
    val username = System.getProperty("user.name")
    try {
        val processBuilder = ProcessBuilder("id", "-u", username)
        val process = processBuilder.start()
        BufferedReader(InputStreamReader(process.inputStream)).use { reader ->
            val line = reader.readLine()
            return line?.toInt() ?: -1
        }
    } catch (e: IOException) {
        e.printStackTrace()
        return -1
    }
}

fun fetchCurrentUserGroupId(): Int {
    val username = System.getProperty("user.name")
    try {
        val processBuilder = ProcessBuilder("id", "-g", username)
        val process = processBuilder.start()
        BufferedReader(InputStreamReader(process.inputStream)).use { reader ->
            val line = reader.readLine()
            return line?.toInt() ?: -1
        }
    } catch (e: IOException) {
        e.printStackTrace()
        return -1
    }
}
