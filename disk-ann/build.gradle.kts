import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import kotlin.io.path.absolute

plugins {
    id("com.google.devtools.ksp")
}

dependencies {
    implementation(libs.commons.rng.simple)
    implementation(libs.commons.rng.sampling)

    testImplementation("commons-net:commons-net:3.9.0")
    testImplementation("org.apache.commons:commons-compress:1.22")
    testImplementation(project(":xodus-utils-test"))
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