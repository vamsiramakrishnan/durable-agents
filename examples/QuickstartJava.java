// Tape quickstart — Java. The same scenario as the Python / TS / Go siblings.
//
//   cd tape/sdk/java && mvn -q -DskipTests package
//   mvn -q dependency:build-classpath -Dmdep.outputFile=target/cp.txt -Dmdep.includeScope=runtime
//   cd ../../../examples && \
//     javac -cp $(cat ../tape/sdk/java/target/cp.txt):../tape/sdk/java/target/classes QuickstartJava.java && \
//     java  -cp .:$(cat ../tape/sdk/java/target/cp.txt):../tape/sdk/java/target/classes QuickstartJava
//
// Or just:  make quickstart-java

import dev.tape.TapeClient;
import dev.tape.proto.*;

public class QuickstartJava {

    private static final String LANG = "java";

    public static void main(String[] args) {
        String url = System.getenv().getOrDefault("TAPE_URL", "tape://127.0.0.1:7878");
        System.out.printf("[quickstart/%s] connecting to %s%n", LANG, url);

        try (TapeClient c = new TapeClient(url)) {
            String invocation = "qs-" + LANG + "-" + (System.currentTimeMillis() / 1000);

            BeginRunResponse run = c.beginRun(
                "quickstart", "quickstart-user",
                invocation, invocation, "qs-" + LANG, 60_000L);
            System.out.printf("[quickstart/%s] begin_run    → run-id=%s%n", LANG, run.getRunId());

            c.recordDecision(run.getRunId(), 0, "quickstart", "{}", "{}", "", "");
            System.out.printf("[quickstart/%s] record_decision  decision_index=0%n", LANG);

            String reqJson = String.format("{\"who\":\"%s\"}", LANG);
            BeginEffectResponse be = c.beginEffect(
                run.getRunId(), 0, "hello", 0, reqJson, "");
            System.out.printf("[quickstart/%s] begin_effect   → key=%s  status=%s%n",
                LANG, be.getIdempotencyKey(), be.getStatus().name());

            String respJson = String.format("{\"ok\":true,\"who\":\"%s\"}", LANG);
            c.completeEffect(run.getRunId(), be.getIdempotencyKey(),
                EffectStatus.EFFECT_STATUS_CONFIRMED, respJson, "");
            System.out.printf("[quickstart/%s] complete_effect → status=CONFIRMED%n", LANG);

            GetEffectResponse got = c.getEffect(run.getRunId(), be.getIdempotencyKey());
            System.out.printf("[quickstart/%s] get_effect     status=%s  response=%s%n",
                LANG, got.getEffect().getStatus().name(), got.getEffect().getResponseJson());
        }
    }
}
