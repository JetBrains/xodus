plugins {
    id("com.github.johnrengelman.shadow")
}

dependencies {
    implementation(project(":xodus-crypto"))
    implementation(project(":xodus-environment"))
    implementation(project(":xodus-entity-store"))
    implementation(project(":xodus-compress"))
    api(project(":xodus-openAPI"))
    implementation(project(":xodus-utils"))
    implementation(libs.slf4j.simple)
}

val testArtifacts: Configuration by configurations.creating

tasks {
    shadowJar {
        mustRunAfter(jar)
        archiveFileName.set(jar.get().archiveFileName)
        manifest {
            attributes["Main-Class"] = "jetbrains.exodus.MainKt"
        }
    }


    jar {
        finalizedBy(shadowJar)
    }

    val jarTest by creating(Jar::class) {
        archiveClassifier.set("test")
        from(sourceSets.test.get().output)
    }

    artifacts {
        add("default", shadowJar)
        add("testArtifacts", jarTest)
    }
}