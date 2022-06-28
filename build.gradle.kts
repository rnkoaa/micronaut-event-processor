plugins {
    id("com.github.johnrengelman.shadow") version "7.1.2"
    id("io.micronaut.application") version "3.4.1"
}

version = "0.1"
group = "io.richard.event"

repositories {
    mavenCentral()
}

dependencies {
    implementation(project(":annotations"))
    implementation(project(":event-common"))
    implementation(project(":event-annotation-processor"))
    annotationProcessor(project(":event-annotation-processor"))

    annotationProcessor("io.micronaut:micronaut-http-validation")
    implementation("io.micronaut:micronaut-http-client")
    implementation("io.micronaut:micronaut-jackson-databind")
    implementation("io.micronaut.kafka:micronaut-kafka")
    implementation("jakarta.annotation:jakarta.annotation-api")
    runtimeOnly("ch.qos.logback:logback-classic")
    implementation("io.micronaut:micronaut-validation")
    testImplementation("org.assertj:assertj-core:3.23.1")

    testImplementation("org.awaitility:awaitility:4.2.0")
    testImplementation("org.testcontainers:testcontainers:1.17.2")
    testImplementation("org.testcontainers:junit-jupiter:1.17.2")
    testImplementation("org.testcontainers:kafka:1.17.2")
}

application {
    mainClass.set("io.richard.event.Application")
}
java {
    sourceCompatibility = JavaVersion.toVersion("17")
    targetCompatibility = JavaVersion.toVersion("17")
}

graalvmNative.toolchainDetection.set(false)
micronaut {
    runtime("netty")
    testRuntime("junit5")
    processing {
        incremental(true)
        annotations("io.richard.event.*")
    }
}



