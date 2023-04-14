dependencies {
    implementation(libs.jetbrains.annotations)
    implementation(libs.slf4j.api)
}

val testArtifacts by configurations.creating

configurations {
    testArtifacts.extendsFrom(testRuntimeOnly.get())
}

tasks {
    val jarTest by creating(Jar::class) {
        archiveClassifier.set("test")
        from(sourceSets.test.get().output)
    }
    artifacts {
        add("testArtifacts", jarTest)
    }
}