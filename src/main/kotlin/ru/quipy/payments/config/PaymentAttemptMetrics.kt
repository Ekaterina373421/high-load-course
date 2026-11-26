package ru.quipy.payments.logic

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.springframework.stereotype.Component
import java.time.Duration
import java.util.concurrent.TimeUnit

/** метрики одной внешней попытки, без ожидания семафора, rate limiter и записи результата. */
@Component
class PaymentAttemptMetrics(private val registry: MeterRegistry) {
    private val outcomes = listOf("success", "provider_failure", "invalid_response", "timeout", "io_error", "error")

    fun registerAccount(account: String) {
        retryCounter(account)
        outcomes.forEach { timer(account, it) }
    }

    /** вызывать перед фактической повторной попыткой, но не перед первой. */
    fun retryStarted(account: String) {
        retryCounter(account).increment()
    }

    fun recordHttp(account: String, outcome: String, elapsedNanos: Long) {
        require(outcome in outcomes)
        timer(account, outcome).record(elapsedNanos.coerceAtLeast(0), TimeUnit.NANOSECONDS)
    }

    private fun retryCounter(account: String): Counter = Counter.builder("payment.retries")
        .description("Explicit repeated payment attempts started; excludes initial attempts and ingress rejections")
        .tag("account", account)
        .register(registry)

    private fun timer(account: String, outcome: String): Timer = Timer.builder("payment.http.attempt")
        .description("HTTP attempt duration including response reading and parsing; excludes queues and persistence")
        .tags("account", account, "outcome", outcome)
        .minimumExpectedValue(Duration.ofMillis(1))
        .maximumExpectedValue(Duration.ofSeconds(60))
        .publishPercentileHistogram()
        .serviceLevelObjectives(
            Duration.ofMillis(50), Duration.ofMillis(100), Duration.ofMillis(250),
            Duration.ofMillis(500), Duration.ofSeconds(1), Duration.ofSeconds(2),
            Duration.ofSeconds(3), Duration.ofSeconds(4), Duration.ofSeconds(6),
            Duration.ofSeconds(10), Duration.ofSeconds(30), Duration.ofSeconds(60),
        )
        .register(registry)
}