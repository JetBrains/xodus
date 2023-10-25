dependencies {
    implementation(libs.spring.boot.starter)
    implementation(libs.grpc.boot.starter)
    implementation(libs.commons.io)

    implementation(project(":xodus-vectoriadb-interface"))
    implementation(project(":xodus-vectoriadb-index"))

    testImplementation(libs.grpc.test)
    testImplementation(libs.spring.boot.starter.test)

    testImplementation(libs.commons.rng.simple)
    testImplementation(libs.commons.rng.sampling)

    testImplementation(libs.commons.net)
    testImplementation(libs.commons.compress)
    testImplementation(libs.commons.lang)
}