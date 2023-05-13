dependencies {
    implementation(libs.ksp.api)
}

tasks.named("clean") {
    onlyIf {
        project.hasProperty("cleanKSP")
    }
}
