dependencies {
    api(project(":xodus-openAPI"))
    implementation(project(":xodus-utils"))
    implementation(project(":xodus-environment"))
    implementation(project(":xodus-compress"))
    implementation(libs.commons.io)
    testImplementation(project(":xodus-utils", "testArtifacts"))
}