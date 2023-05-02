dependencies {
    testImplementation(project(":xodus-utils-test"))
    testImplementation(project(":xodus-utils"))
    testImplementation(project(":xodus-environment"))
}

tasks.test {
    testLogging.showStandardStreams = true
}