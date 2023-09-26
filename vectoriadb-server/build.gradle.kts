dependencies {
    implementation(libs.spring.boot.starter)
    implementation(libs.grpc.boot.starter)
    implementation(project(":xodus-vectoriadb-interface"))
    implementation(project(":xodus-vectoriadb-index"))
}