dependencies {
    implementation(project(":xodus-entity-store"))
    implementation(project(":xodus-utils"))
    implementation("com.orientechnologies:orientdb-core:4.0.0-SNAPSHOT")
    api(project(":xodus-openAPI"))
    implementation("com.github.penemue:keap:0.3.0")

    testImplementation(project(":xodus-utils", "testArtifacts"))
    testImplementation(project(":xodus-entity-store", "testArtifacts"))
}