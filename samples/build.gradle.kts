dependencies {
    implementation(project(":xodus-entity-store"))
    implementation(project(":xodus-crypto"))
    implementation(project(":xodus-environment"))
    implementation("org.slf4j:slf4j-jdk14:2.0.7")
}

tasks {
    jar {
        enabled = false
    }
}
