import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import kotlin.io.path.absolute

plugins {
    id("com.google.devtools.ksp")
}

dependencies {
    implementation(libs.commons.rng.simple)
    implementation(libs.commons.rng.sampling)
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