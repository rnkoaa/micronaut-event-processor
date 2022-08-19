plugins {
    id("com.github.johnrengelman.shadow") version "7.1.2"
    id("io.micronaut.application") version "3.4.1"
    id("groovy")
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
    annotationProcessor(project(":micronaut-proxy-provider"))

    implementation("jakarta.annotation:jakarta.annotation-api:2.1.1")
    implementation("org.jetbrains:annotations:23.0.0")

    annotationProcessor("io.micronaut:micronaut-http-validation")
    implementation("io.micronaut:micronaut-http-client")
    implementation("io.micronaut:micronaut-jackson-databind")
    implementation("io.micronaut.kafka:micronaut-kafka:4.4.0")
    implementation("jakarta.annotation:jakarta.annotation-api")
    runtimeOnly("ch.qos.logback:logback-classic")
    implementation("io.micronaut:micronaut-validation")

    testImplementation("org.assertj:assertj-core:3.23.1")
    testImplementation("org.awaitility:awaitility:4.2.0")
    testImplementation("org.testcontainers:testcontainers:1.17.3")
    testImplementation("org.testcontainers:spock:1.17.3")
    testImplementation("org.testcontainers:kafka:1.17.3")
}

application {
    mainClass.set("io.richard.event.Application")
}
java {
    sourceCompatibility = JavaVersion.toVersion("18")
    targetCompatibility = JavaVersion.toVersion("18")
}

graalvmNative.toolchainDetection.set(false)
micronaut {
    runtime("netty")
    testRuntime("spock2")
    processing {
        incremental(true)
        annotations("io.richard.event.*")
    }
}



