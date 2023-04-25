dependencies {
    implementation(project(":xodus-entity-store"))
    implementation(project(":xodus-utils"))
    api(project(":xodus-openAPI"))

    testImplementation(project(":xodus-utils", "testArtifacts"))
    testImplementation(project(":xodus-entity-store", "testArtifacts"))
}