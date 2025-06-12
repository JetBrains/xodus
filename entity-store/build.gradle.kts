val ytdbVersion = "1.0.0-20250605.144611-39"
val ktorVersion = "3.1.3"

dependencies {
    api(project(":xodus-openAPI"))
    api("io.youtrackdb:youtrackdb-core:$ytdbVersion")
    api("io.youtrackdb:youtrackdb-server:$ytdbVersion")

    implementation(project(":xodus-utils"))
    implementation(project(":xodus-environment"))
    implementation(project(":xodus-compress"))
    implementation(libs.commons.io)

    testImplementation("io.github.classgraph:classgraph:4.8.112")
    testImplementation(project(":xodus-utils", "testArtifacts"))
    testImplementation(libs.mockk)
    testImplementation(libs.truth)
    testImplementation(kotlin("test"))
    testImplementation("io.ktor:ktor-client-core:$ktorVersion")
    testImplementation("io.ktor:ktor-client-java:$ktorVersion")
    testImplementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.8.1")

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
