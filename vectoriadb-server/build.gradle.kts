import com.bmuschko.gradle.docker.tasks.image.DockerBuildImage

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
    register<Copy>("copyToLibs") {
        dependsOn("build")
        from(project.configurations.getByName("runtimeClasspath"))
        into(project.file("build/libs"))
    }
    register<DockerBuildImage>("buildDockerImage") {
        dependsOn("copyToLibs")
        inputDir = project.projectDir
        dockerFile = project.file("src/main/docker/Dockerfile")
        images.add("vectoriadb/vectoriadb-server:latest")
    }
}
