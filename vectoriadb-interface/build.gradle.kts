import com.google.protobuf.gradle.*

plugins {
    id("com.google.protobuf")
    id("org.jetbrains.gradle.plugin.idea-ext")
}

dependencies {
    implementation(libs.protobuf.java)
    implementation(libs.grpc.stub)
    implementation(libs.grpc.protobuf)
    compileOnly(libs.javax.annotation.api)
}

protobuf {
    protoc {
        artifact = libs.protobuf.protoc.get().toString()
    }
    plugins {
        id("grpc") {
            artifact = libs.grpc.java.get().toString()
        }
    }

    generateProtoTasks {
        ofSourceSet("main").forEach {
            it.plugins {
                id("grpc") {}
            }
        }
    }
}

sourceSets {
    main {
        java {
            srcDirs += file("build/generated/source/proto/main/grpc")
            srcDirs += file("build/generated/source/proto/main/java")
        }
    }
}