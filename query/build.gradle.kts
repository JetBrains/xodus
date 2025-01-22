dependencies {
    implementation(project(":xodus-entity-store"))
    implementation(project(":xodus-persistent-entity-store"))
    implementation(project(":xodus-utils"))
    api(project(":xodus-openAPI"))
    implementation("com.github.penemue:keap:0.3.0")
    implementation(project(":xodus-environment"))
    implementation("commons-io:commons-io:2.15.1")
    implementation(libs.slf4j.simple)

    testImplementation(project(":xodus-utils", "testArtifacts"))
    testImplementation(project(":xodus-entity-store", "testArtifacts"))
    testImplementation(project(":xodus-persistent-entity-store", "testArtifacts"))
    testImplementation(libs.mockk)
    testImplementation(libs.truth)
    testImplementation(kotlin("test"))
}

tasks {
    register<JavaExec>("migrateXodusToOrient") {
        group = "application"
        mainClass = "jetbrains.exodus.query.metadata.MigrateXodusToOrientKt"
        classpath = sourceSets["main"].runtimeClasspath
        jvmArgs = listOf(
            "-server",
            "-Xmx16g",
            "-XX:+HeapDumpOnOutOfMemoryError",
        )
        systemProperties = mapOf(
            "xodusDatabaseDirectory" to (project.findProperty("xodusDatabaseDirectory")),
            "xodusStoreName" to (project.findProperty("xodusStoreName")),
            "xodusCipherKey" to (project.findProperty("xodusCipherKey")),
            "xodusCipherIV" to (project.findProperty("xodusCipherIV")),
            "xodusMemoryUsagePercentage" to (project.findProperty("xodusMemoryUsagePercentage")),

            "orientDatabaseType" to (project.findProperty("orientDatabaseType")),
            "orientDatabaseDirectory" to (project.findProperty("orientDatabaseDirectory")),
            "orientDatabaseName" to (project.findProperty("orientDatabaseName")),
            "orientUsername" to (project.findProperty("orientUsername")),
            "orientPassword" to (project.findProperty("orientPassword")),

            "validateDataAfterMigration" to (project.findProperty("validateDataAfterMigration")),
            "entitiesPerTransaction" to (project.findProperty("entitiesPerTransaction")),
        )
    }
}
