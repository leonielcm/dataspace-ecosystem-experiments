package org.eclipse.edc.test.system.perf;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PerfConfigTest {

    @Test
    void from_noProperty_playgroundDefaults() {
        var config = PerfConfig.from(key -> null, "alice", "192-168-1-220.nip.io");

        assertThat(config.target()).isEqualTo(PerfConfig.Target.PLAYGROUND);
        assertThat(config.warmup()).isEqualTo(20);
        assertThat(config.runs()).isEqualTo(200);
        assertThat(config.coldRuns()).isEqualTo(5);
        assertThat(config.rejectionRuns()).isEqualTo(20);
        assertThat(config.sizes()).containsExactly(1024L, 100 * 1024L, 1024 * 1024L, 10 * 1024 * 1024L);
        assertThat(config.retirementPhase()).isTrue();
        assertThat(config.providerPublicEnabled()).isTrue();
        assertThat(config.playground().providerNamespace()).isEqualTo("alice-dse-provider");
        assertThat(config.playground().backendService()).isEqualTo("svc/provider-backend");
        assertThat(config.playground().providerPublicIngressUrl()).isEqualTo("http://192-168-1-220.nip.io/provider/dp/public");
    }

    @Test
    void from_providerPublicDisabled_notEnabled() {
        var config = PerfConfig.from(Map.of("perf.provider.public.mode", "disabled")::get, "alice", "host");

        assertThat(config.providerPublicEnabled()).isFalse();
    }

    @Test
    void from_uatWithEveryRequiredProperty_noSizesNoRetirementNoProviderPublic() {
        var properties = uatProperties();
        properties.put("perf.uat.direct.headers", "X-Api-Key: secret-value; Accept: application/json");

        var config = PerfConfig.from(properties::get, "alice", "host");

        assertThat(config.target()).isEqualTo(PerfConfig.Target.UAT);
        assertThat(config.sizes()).containsExactly(-1L);
        assertThat(config.retirementPhase()).isFalse();
        assertThat(config.providerPublicEnabled()).isFalse();
        assertThat(config.uat().directHeaders()).containsExactly(
                Map.entry("X-Api-Key", "secret-value"), Map.entry("Accept", "application/json"));
    }

    @Test
    void from_uatMissingProperties_listsThem() {
        var properties = uatProperties();
        properties.remove("perf.uat.assetId");
        properties.remove("perf.uat.direct.url");

        assertThatThrownBy(() -> PerfConfig.from(properties::get, "alice", "host"))
                .hasMessageContaining("[perf.uat.assetId, perf.uat.direct.url]");
    }

    @Test
    void toString_uat_hidesApiKeyAndHeaderValues() {
        var properties = uatProperties();
        properties.put("perf.uat.apiKey", "consumer-api-key");
        properties.put("perf.uat.direct.headers", "X-Api-Key: provider-api-key");

        var config = PerfConfig.from(properties::get, "alice", "host");

        assertThat(config.toString()).doesNotContain("consumer-api-key", "provider-api-key").contains("X-Api-Key");
        assertThat(config.describe().toString()).doesNotContain("consumer-api-key", "provider-api-key");
    }

    @Test
    void parseSizes_suffixesAndSpaces() {
        assertThat(PerfConfig.parseSizes(" 0, 512 ,2k,3M ")).isEqualTo(List.of(0L, 512L, 2048L, 3L * 1024 * 1024));
    }

    @Test
    void parseHeaders_withoutSeparator_rejected() {
        assertThatThrownBy(() -> PerfConfig.parseHeaders("NoColon"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static Map<String, String> uatProperties() {
        var properties = new HashMap<String, String>();
        properties.put("perf.target", "uat");
        properties.put("perf.uat.consumer.managementUrl", "https://uat/consumer/cp/management");
        properties.put("perf.uat.consumer.dataUrl", "https://uat/consumer/dp/data");
        properties.put("perf.uat.provider.did", "did:web:provider");
        properties.put("perf.uat.provider.dspUrl", "https://uat/provider/cp/dsp");
        properties.put("perf.uat.assetId", "some-asset");
        properties.put("perf.uat.direct.url", "https://api.example/resource");
        return properties;
    }
}
