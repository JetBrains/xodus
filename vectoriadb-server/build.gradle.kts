dependencies {
    implementation(libs.spring.boot.starter) {
        exclude(group = "org.springframework.boot", module = "spring-boot-starter-logging")
    }
    implementation(libs.spring.boot.starter.log4j2)
    implementation(libs.log4j2.slf4j)
    implementation(libs.grpc.boot.starter)

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


tasks.test {
    systemProperty(
        "vectoriadb.log.dir",
        File(project.layout.buildDirectory.asFile.get(), "logs").absoluteFile
    )
}