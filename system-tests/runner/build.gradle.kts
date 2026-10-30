plugins {
    `java-library`
    `java-test-fixtures`
}

// Configure test task to pass system properties
tasks.test {
    // Not excluded by integration-tests.gradle's EndToEndTest(s) filename pattern (that init
    // script never applies here) -- excluded directly so a known-limitation-tagged test
    // (e.g. #971) doesn't fail test:e2e while the underlying gap is still open.
    useJUnitPlatform {
        excludeTags("known-limitation")
        // ConnectorPerformanceTest is an experiment, not a regression test: it is also tagged
        // EndToEndTest (and CI runs with -DrunAllTests=true), so it is only kept when asked for
        // explicitly with -DincludeTags=performance.
        val includeTags = System.getProperty("includeTags", "").split(",").map { it.trim() }
        if ("performance" !in includeTags) {
            excludeTags("performance")
        }
    }
    // Pass all system properties that start with "cluster.", "kubectl.", "developer.", "ssh."
    // or "perf." to the test JVM.
    System.getProperties().forEach { key, value ->
        if (key.toString().startsWith("cluster.") || key.toString().startsWith("kubectl.") ||
            key.toString().startsWith("developer.") || key.toString().startsWith("ssh.") ||
            key.toString().startsWith("perf.")) {
            systemProperty(key.toString(), value.toString())
        }
    }
}

dependencies {
    testImplementation(libs.edc.spi.identity.did)
    testImplementation(libs.awaitility)
    testImplementation(libs.edc.spi.dpf)
    testImplementation(libs.edc.spi.vc)
    testImplementation(libs.jakarta.rsApi)
    testImplementation(libs.edc.lib.keys)
    testImplementation(project(":spi:telemetry-service-spi"))
    testImplementation(project(":extensions:common:policies"))
    testImplementation(project(":spi:common-spi"))
    testImplementation(libs.jjwt.api)
    testImplementation(libs.jjwt.impl)
    testImplementation(libs.jjwt.jackson)

    testImplementation(libs.postgres)

    testFixturesApi(libs.restAssured)
    testFixturesApi(testFixtures(libs.edc.ext.api.management.test.fixtures))

    testFixturesImplementation(project(":spi:common-spi"))
    testFixturesImplementation(project(":extensions:common:policies"))
    testFixturesImplementation(project(":extensions:issuer-service:domain-attestation-api"))
    testFixturesImplementation(project(":extensions:issuer-service:membership-attestation-api"))
    testFixturesImplementation(libs.edc.issuerservice.api.credentialdefinition)
    testFixturesImplementation(libs.edc.issuerservice.api.attestation)
    testFixturesImplementation(libs.edc.issuerservice.api.holder)
    testFixturesImplementation(libs.edc.identityhub.api.verifiablecredentials)
    testFixturesImplementation(libs.edc.spi.vc)
    testFixturesImplementation(libs.edc.lib.crypto.common)
    testFixturesImplementation(libs.edc.core.token)
    testFixturesImplementation(libs.edc.lib.token)
    testFixturesImplementation(libs.edc.spi.jsonld)
    testFixturesImplementation(libs.edc.federatedcatalog.spi.core)
    testFixturesImplementation(libs.edc.identityhub.spi.core)
    testFixturesImplementation(libs.edc.spi.identity.did)
    testFixturesImplementation(libs.awaitility)
    testImplementation(libs.edc.core.junit)
    testImplementation(libs.mockito.junit.jupiter)
    testImplementation(libs.junit.jupiter.api)
    testImplementation(libs.junit.jupiter.params)
    testImplementation(libs.testcontainers.junit.jupiter)
    testRuntimeOnly(libs.junit.jupiter.engine)
    testImplementation(libs.assertj)
    testImplementation(libs.kafka.clients)
    testFixturesImplementation(libs.kafka.clients)

}
