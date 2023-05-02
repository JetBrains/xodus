dependencies {
    implementation(project(":xodus-entity-store"))
    implementation(project(":xodus-utils"))
    api(project(":xodus-openAPI"))
    implementation("com.github.penemue:keap:0.3.0")

    testImplementation(project(":xodus-utils-test"))
    testImplementation(project(":xodus-entity-store", "testArtifacts"))
}