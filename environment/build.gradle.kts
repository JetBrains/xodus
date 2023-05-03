import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

dependencies {
    implementation(project(":xodus-compress"))
    implementation(project(":xodus-utils"))
    api(project(":xodus-openAPI"))
    implementation(libs.jetbrains.annotations)

    testImplementation(project(":xodus-utils-test"))
    testImplementation(project(":xodus-environment-test"))
}


tasks {
    named("compileKotlin", KotlinCompile::class) {
        kotlinOptions {
            allWarningsAsErrors = true
        }
    }
}