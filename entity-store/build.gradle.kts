dependencies {
    api(project(":xodus-openAPI"))
    api("com.orientechnologies:orientdb-core:4.0.0-20241105.082455-189")

    implementation(project(":xodus-utils"))
    implementation(project(":xodus-environment"))
    implementation(project(":xodus-compress"))
    implementation(libs.commons.io)

    testImplementation("io.github.classgraph:classgraph:4.8.90")
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