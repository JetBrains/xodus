import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

dependencies {
    implementation(project(":xodus-compress"))
    implementation(project(":xodus-utils"))
    api(project(":xodus-openAPI"))
    implementation(libs.jetbrains.annotations)
    testImplementation(project(":xodus-utils", "testArtifacts"))
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

    named("compileKotlin", KotlinCompile::class) {
        kotlinOptions {
            allWarningsAsErrors = true
        }
    }
}