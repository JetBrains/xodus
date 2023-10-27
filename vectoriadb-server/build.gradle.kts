dependencies {
    implementation(libs.spring.boot.starter)
    implementation(libs.grpc.boot.starter)
    implementation(libs.commons.io)

    implementation(project(":xodus-vectoriadb-interface"))
    implementation(project(":xodus-vectoriadb-index"))

    testImplementation(libs.grpc.test)
    testImplementation(libs.spring.boot.starter.test)
}