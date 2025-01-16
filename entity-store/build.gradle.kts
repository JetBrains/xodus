dependencies {
    api(project(":xodus-openAPI"))
    api("io.youtrackdb:youtrackdb-core:1.0.0-20250116.142259-7")

    implementation(project(":xodus-utils"))
    implementation(project(":xodus-environment"))
    implementation(project(":xodus-compress"))
    implementation(libs.commons.io)

    testImplementation("io.github.classgraph:classgraph:4.8.112")
    testImplementation(project(":xodus-utils", "testArtifacts"))
    testImplementation(libs.mockk)
    testImplementation(libs.truth)
    testImplementation(kotlin("test"))
}

val testArtifacts: Configuration by configurations.creating

tasks {
    val jarTest by creating(Jar::class) {
        archiveClassifier.set("test")
        from(sourceSets.test.get().output)
    }

    artifacts {
        add("testArtifacts", jarTest)
    }
}
