plugins {
    `java-library`
}

// Control plane runtime of the MCP end-to-end test: the EDC control plane, with the mock identity service instead
// of the decentralized claims (DCP) stack, so that no identity hub nor credential is needed. In-memory stores.
dependencies {
    runtimeOnly(libs.edc.controlplane.base.bom) {
        // same single-participant setup as the control plane launcher
        exclude(group = "org.eclipse.edc", module = "participant-context-single-core")
    }
    runtimeOnly(project(":extensions:common:participant-context-single-vault-seed"))
    runtimeOnly(libs.edc.participant.context.config.core)
    runtimeOnly(libs.edc.iam.mock)
}

edcBuild {
    publish.set(false)
}
