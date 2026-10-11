package ru.quipy.payments.logic

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.stereotype.Component

@Component
class PaymentFlowMetrics(registry: MeterRegistry) {
    private val received = counter(registry, "received")
    private val sent = counter(registry, "sent")
    private val completed = counter(registry, "completed")
    private val rejectedDeadline = counter(registry, "rejected_deadline")

    fun received() = received.increment()

    fun sent() = sent.increment()

    fun completed() = completed.increment()

    fun rejectedDeadline() = rejectedDeadline.increment()

    private fun counter(registry: MeterRegistry, stage: String): Counter =
        Counter.builder("payment.flow")
            .description("Payment flow events by stage; completed includes all external call outcomes")
            .tag("stage", stage)
            .register(registry)
}