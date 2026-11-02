rootProject.name = "dataspace-ecosystem"

include(":core")
include(":core:telemetry-agent-core")
include(":core:telemetry-service-core")
include(":core:common:telemetry-record-store")
include(":core:kafka-proxy-k8s-manager-core")

include(":extensions")
include(":extensions:agreements")
include(":extensions:agreements:retirement-evaluation-core")
include(":extensions:agreements:retirement-evaluation-api")
include(":extensions:agreements:retirement-evaluation-spi")
include(":extensions:agreements:retirement-evaluation-store-sql")

include(":extensions:common:vc-revocation-patch")
include(":extensions:common:policies")
include(":extensions:common:odrl-policy-did-validation")
include(":extensions:control-plane:policy-definition-iri-right-operand")
include(":extensions:common:metrics:custom-micrometer")
include(":extensions:common:store:sql:telemetry-store-sql")
include(":extensions:common:participant-context-single-vault-seed")
include(":extensions:common:migration:flyway-core")
include(":extensions:common:migration:migration-shared")

include(":extensions:identity-hub:did-web-parser")
include(":extensions:identity-hub:identity-hub-iatp")
include(":extensions:identity-hub:superuser-seed")
include(":extensions:identity-hub:participant-activation-healer")
include(":extensions:identity-hub:identity-hub-migration")

include(":extensions:data-plane:data-plane-public-api-v2")
include(":extensions:data-plane:data-plane-data-consumption-metrics")
include(":extensions:data-plane:data-plane-http-header-secrets")
include(":extensions:data-plane:sftp-source")
include(":extensions:data-plane:data-plane-migration")

include(":extensions:control-plane:asset-custom-property-subscriber")
include(":extensions:control-plane:transfer-data-plane-signal-kafka")
include(":extensions:control-plane:control-plane-federated-catalog-filter")
include(":extensions:control-plane:control-plane-migration")

include(":extensions:issuer-service:membership-attestation-api")
include(":extensions:issuer-service:membership-attestation-store-sql")

include(":extensions:issuer-service:domain-attestation-api")
include(":extensions:issuer-service:domain-attestation-store-sql")
include(":extensions:issuer-service:issuer-service-migration")

include(":extensions:federated-catalog:participant-registry-node-directory")
include(":extensions:federated-catalog:filter")

include(":extensions:telemetry-agent")
include(":extensions:telemetry-agent:kafka-telemetry-record-publisher")
include(":extensions:telemetry-agent:telemetry-agent-migration")
include(":extensions:telemetry-test-utils")

include(":extensions:telemetry-service")
include(":extensions:telemetry-service:telemetry-service-credential-api")
include(":extensions:telemetry-service:telemetry-service-credential-jwt")

include(":extensions:telemetry-storage")
include(":extensions:telemetry-storage:telemetry-storage-api")
include(":extensions:telemetry-storage:telemetry-storage-store-sql")
include(":extensions:telemetry-storage:telemetry-storage-kafka-consumer")

include(":extensions:telemetry-csv-manager")
include(":extensions:telemetry-csv-manager:telemetry-csv-manager-api")

include(":spi")
include(":spi:common-spi")
include(":spi:migration-spi")
include(":spi:telemetry-agent-spi")
include(":spi:issuer-service-spi")
include(":spi:telemetry-service-spi")
include(":spi:telemetry-storage-spi")
include(":spi:federated-catalog-filter-spi")

include(":launchers")

include(":launchers:control-plane")
include(":launchers:control-plane:control-plane-base")
include(":launchers:control-plane:control-plane-postgresql-hashicorpvault")

include(":launchers:data-plane")
include(":launchers:data-plane:data-plane-base")
include(":launchers:data-plane:data-plane-postgresql-hashicorpvault")

include(":launchers:identity-hub")
include(":launchers:identity-hub:identity-hub-base")
include(":launchers:identity-hub:identity-hub-postgresql-hashicorpvault")

include(":launchers:federated-catalog")
include(":launchers:federated-catalog:federated-catalog-base")
include(":launchers:federated-catalog:federated-catalog-postgresql-hashicorpvault")
include(":launchers:federated-catalog-filter")
include(":launchers:federated-catalog-filter:federated-catalog-filter-base")
include(":launchers:federated-catalog-filter:federated-catalog-filter-postgresql-hashicorpvault")

include(":launchers:issuer-service")
include(":launchers:issuer-service:issuer-service-base")
include(":launchers:issuer-service:issuer-service-postgresql-hashicorpvault")

include(":launchers:telemetry-service")
include(":launchers:telemetry-service:telemetry-service-base")
include(":launchers:telemetry-service:telemetry-service-postgresql-hashicorpvault")

include(":launchers:telemetry-agent")
include(":launchers:telemetry-agent:telemetry-agent-base")
include(":launchers:telemetry-agent:telemetry-agent-postgresql-hashicorpvault")

include(":launchers:telemetry-storage")
include(":launchers:telemetry-storage:telemetry-storage-base")
include(":launchers:telemetry-storage:telemetry-storage-postgresql-hashicorpvault")
include(":launchers:kafka-proxy-k8s-manager")
include(":launchers:kafka-proxy-k8s-manager:kafka-proxy-k8s-manager-base")

include(":plugins")

include(":launchers:telemetry-csv-manager")
include(":launchers:telemetry-csv-manager:telemetry-csv-manager-base")
include(":launchers:telemetry-csv-manager:telemetry-csv-manager-postgresql-hashicorpvault")

include(":system-tests:backend-service-provider")
include(":system-tests:runner")
include(":system-tests:mcp-e2e:control-plane")
include(":system-tests:mcp-e2e:data-plane")
include(":system-tests:mcp-e2e:runner")

pluginManagement {
    repositories {
        mavenLocal()
        gradlePluginPortal()
        mavenCentral()
        maven {
            url = uri("https://central.sonatype.com/repository/maven-snapshots/")
        }
    }
}
dependencyResolutionManagement {
    repositories {
        mavenLocal()
        gradlePluginPortal()
        mavenCentral()
        maven {
            url = uri("https://central.sonatype.com/repository/maven-snapshots/")
        }
    }
}
include("launchers:federated-catalog-filter")
include("launchers:federated-catalog-filter:federated-catalog-filter-base")
findProject(":launchers:federated-catalog-filter:federated-catalog-filter-base")?.name = "federated-catalog-filter-base"
include("launchers:federated-catalog-filter:federated-catalog-filter-postgress-hashicorp")
findProject(":launchers:federated-catalog-filter:federated-catalog-filter-postgress-hashicorp")?.name = "federated-catalog-filter-postgress-hashicorp"

include("extensions:telemetry-test-utils")