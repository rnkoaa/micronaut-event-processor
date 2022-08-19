
plugins {
    id("io.richard.event.processor.java-library-conventions")
    id("io.micronaut.library") //version "2.0.8"
}

dependencies {
    implementation("org.slf4j:slf4j-api")
    implementation("org.slf4j:slf4j-simple")

    annotationProcessor("io.micronaut:micronaut-inject-java")
    implementation("io.micronaut:micronaut-inject-java")

    implementation(project(":annotations"))
    annotationProcessor(project(":event-annotation-processor"))
//    annotationProcessor(project(":kafka-event-annotation-processor"))
    implementation("jakarta.inject:jakarta.inject-api:2.0.1")

    testImplementation("org.junit.jupiter:junit-jupiter-api")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
}