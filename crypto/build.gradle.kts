dependencies {
    implementation(project(":xodus-environment"))
    implementation(project(":xodus-entity-store"))
    implementation(project(":xodus-utils"))
    api(project(":xodus-openAPI"))
    implementation(libs.bouncyCastle)
    testImplementation(project(":xodus-utils-test"))
    testImplementation(project(":xodus-openAPI", "testArtifacts"))
}