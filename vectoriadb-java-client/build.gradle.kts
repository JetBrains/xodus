dependencies {
    implementation(libs.grpc.java)
    implementation(libs.grpc.protobuf)
    implementation(libs.grpc.netty.shaded)
    implementation(libs.grpc.stub)
    implementation(libs.commons.net)

    implementation(project(":vectoriadb-interface"))
}