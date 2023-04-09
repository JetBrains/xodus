dependencies {
    implementation(libs.jetbrains.annotations)
    implementation(project(":xodus-utils"))
    testImplementation(project(":xodus-utils", "testArtifacts"))
}

val testArtifacts by configurations.creating

tasks {
    val jarTest by creating(Jar::class) {
        archiveClassifier.set("test")
        from(sourceSets.test.get().output)
    }
    artifacts {
        add("testArtifacts", jarTest)
    }
}