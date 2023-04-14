dependencies {
    testImplementation(project(":xodus-utils", "testArtifacts"))
    testImplementation(project(":xodus-utils"))
    testImplementation(project(":xodus-environment"))
}

tasks.test {
    testLogging.showStandardStreams = true
}