dependencies {
    implementation(project(":xodus-environment"))
    api(project(":xodus-openAPI"))
    implementation(project(":xodus-utils"))
    implementation(libs.jetbrains.annotations)

    implementation(libs.lucene.core)
    implementation(libs.lucene.analyzers.common)
    implementation(libs.lucene.queries)
    implementation(libs.lucene.queryparser)

    testImplementation(project(":xodus-utils-test"))
    testImplementation(libs.lucene.test.framework)
    testImplementation(project(":xodus-environment-test"))
}