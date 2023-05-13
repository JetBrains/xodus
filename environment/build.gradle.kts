import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import kotlin.io.path.absolute

plugins {
    id("com.google.devtools.ksp")
}

dependencies {
    implementation(project(":xodus-compress"))
    implementation(project(":xodus-utils"))
    api(project(":xodus-openAPI"))
    implementation(libs.jetbrains.annotations)

    ksp(project(":xodus-ksp-plugin"))

    testImplementation(project(":xodus-utils-test"))
    testImplementation(project(":xodus-environment-test"))
}

ksp {
    arg("excludePaths", project.projectDir.toPath().resolve("src").resolve("test").absolute().toString())
}

tasks {
    named("compileKotlin", KotlinCompile::class) {
        kotlinOptions {
            allWarningsAsErrors = true
        }
    }
}