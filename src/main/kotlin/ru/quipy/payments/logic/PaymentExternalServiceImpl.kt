package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import okhttp3.Call
import okhttp3.Callback
import okhttp3.Dispatcher
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import okhttp3.Response
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicBoolean


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val meterRegistry: MeterRegistry,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()

        private const val IDLE_SLEEP_MS = 5L
    }

    private data class PendingPayment(
        val paymentId: UUID,
        val amount: Int,
        val paymentStartedAt: Long,
        val deadline: Long,
    )

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    // кейс 1. создаем rate limiter с лимитом из настроек аккаунта
    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))

    // Кейс 2 (acc-5): реальный потолок = min(rate, parallel / avgTime) = min(3, 5 / 4.9) ~ 1.02 rps.
    // Узкое место - окно параллельных запросов, rate limiter остается страховкой.
    private val window = NonBlockingOngoingWindow(parallelRequests)
    private val queue = LinkedBlockingQueue<PendingPayment>()
    private val inFlight = AtomicInteger(0)
    private val dispatcher = Executors.newSingleThreadExecutor(NamedThreadFactory("payment-dispatcher-$accountName"))
    private val senders = Executors.newFixedThreadPool(4, NamedThreadFactory("payment-sender-$accountName"))

    // Таймаут вызова: 3 * среднее время обработки (~14.7s для acc-5)
    private val callTimeout = requestAverageProcessingTime.multipliedBy(3)

    private val client = OkHttpClient.Builder()
        .callTimeout(callTimeout)
        .dispatcher(Dispatcher().apply {
            maxRequests = parallelRequests
            maxRequestsPerHost = parallelRequests
        })
        .build()

    private val callTimer: Timer = Timer.builder("payment_external_call_duration")
        .description("Duration of calls to the external payment system")
        .tag("account", accountName)
        .publishPercentileHistogram()
        .maximumExpectedValue(Duration.ofSeconds(60))
        .register(meterRegistry)

    private val queueWaitTimer: Timer = Timer.builder("payment_queue_wait_duration")
        .description("Time from payment start to submission to the external system")
        .tag("account", accountName)
        .publishPercentileHistogram()
        .minimumExpectedValue(Duration.ofMillis(100))
        .maximumExpectedValue(Duration.ofSeconds(120))
        .register(meterRegistry)

    init {
        Gauge.builder("payment_queue_size", queue) { it.size.toDouble() }
            .tag("account", accountName).register(meterRegistry)
        Gauge.builder("payment_in_flight", inFlight) { it.get().toDouble() }
            .tag("account", accountName).register(meterRegistry)

        dispatcher.execute { dispatchLoop() }
    }

    private fun countOutcome(outcome: String) {
        Counter.builder("payment_external_requests_total")
            .tag("account", accountName)
            .tag("outcome", outcome)
            .register(meterRegistry)
            .increment()
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        queue.put(PendingPayment(paymentId, amount, paymentStartedAt, deadline))
    }

    private fun dispatchLoop() {
        while (!Thread.currentThread().isInterrupted) {
            try {
                val payment = queue.poll(100, TimeUnit.MILLISECONDS) ?: continue
                awaitSlotAndSend(payment)
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
            } catch (e: Exception) {
                logger.error("[$accountName] Dispatcher error", e)
            }
        }
    }

    private fun awaitSlotAndSend(payment: PendingPayment) {
        while (true) {
            if (now() + requestAverageProcessingTime.toMillis() > payment.deadline) {
                rejectByDeadline(payment)
                return
            }

            if (window.putIntoWindow() is NonBlockingOngoingWindow.WindowResponse.Success) {
                if (rateLimiter.tick()) {
                    inFlight.incrementAndGet()
                    senders.execute { send(payment) }
                    return
                }
                window.releaseWindow()
            }
            Thread.sleep(IDLE_SLEEP_MS)
        }
    }

    private fun rejectByDeadline(payment: PendingPayment) {
        countOutcome("dropped_deadline")
        val transactionId = UUID.randomUUID()
        logger.warn("[$accountName] Payment ${payment.paymentId} dropped: deadline is unreachable")
        senders.execute {
            try {
                logSubmission(payment.paymentId, transactionId, false, payment.paymentStartedAt)
                logProcessing(payment.paymentId, transactionId, false, reason = "Deadline exceeded before submission.")
            } catch (e: Exception) {
                logger.error("[$accountName] Failed to log dropped payment ${payment.paymentId}", e)
            }
        }
    }

    private fun send(payment: PendingPayment) {
        val paymentId = payment.paymentId
        val transactionId = UUID.randomUUID()
        var enqueued = false
        val released = AtomicBoolean(false)

        fun releaseSlot() {
            if (released.compareAndSet(false, true)) finishCall()
        }

        queueWaitTimer.record(Duration.ofMillis(now() - payment.paymentStartedAt))
        try {
            logSubmission(paymentId, transactionId, true, payment.paymentStartedAt)
            logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

            val request = Request.Builder().run {
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=${payment.amount}")
                post(emptyBody)
            }.build()

            val sample = Timer.start(meterRegistry)
            enqueued = true
            client.newCall(request).enqueue(object : Callback {

                override fun onFailure(call: Call, e: IOException) {
                    try {
                        sample.stop(callTimer)
                        releaseSlot()

                        val timeout = e is SocketTimeoutException || e.message?.contains("timeout", true) == true
                        countOutcome(if (timeout) "timeout" else "error")

                        logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                        logProcessing(paymentId, transactionId, false, if (timeout) "Request timeout." else e.message)
                    } finally {
                        releaseSlot()
                    }
                }

                override fun onResponse(call: Call, response: Response) {
                    try {
                        sample.stop(callTimer)
                        response.use {
                            val raw = it.body?.string()
                            releaseSlot()

                            val body = try {
                                mapper.readValue(raw, ExternalSysResponse::class.java)
                            } catch (e: Exception) {
                                logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${it.code}, reason: $raw")
                                ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                            }

                            countOutcome(if (body.result) "success" else "declined")
                            logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")
                            logProcessing(paymentId, transactionId, body.result, body.message)
                        }
                    } catch (e: Exception) {
                        logger.error("[$accountName] Response handling failed for txId: $transactionId, payment: $paymentId", e)
                    } finally {
                        releaseSlot()
                    }
                }
            })
        } catch (e: Exception) {
            logger.error("[$accountName] Payment failed before sending, txId: $transactionId, payment: $paymentId", e)

            if (!enqueued) {
                countOutcome("error")
                try {
                    logProcessing(paymentId, transactionId, false, e.message)
                } finally {
                    releaseSlot()
                }
            }
        }
    }

    // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
    // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
    private fun logProcessing(paymentId: UUID, transactionId: UUID, success: Boolean, reason: String?) {
        paymentESService.update(paymentId) {
            it.logProcessing(success, now(), transactionId, reason = reason)
        }
    }

    // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
    // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
    private fun logSubmission(paymentId: UUID, transactionId: UUID, success: Boolean, paymentStartedAt: Long) {
        paymentESService.update(paymentId) {
        it.logSubmission(success, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
    }}

    private fun finishCall() {
        inFlight.decrementAndGet()
        window.releaseWindow()
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()