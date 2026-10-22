package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.atomic.AtomicInteger


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val paymentFlowMetrics: PaymentFlowMetrics,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests
    private val expectedProcessingMillis = requestAverageProcessingTime.toMillis()

    // кейс 1. создаем rate limiter с лимитом из настроек аккаунта
    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))

    // кейс 2. создаем окно in-flight запросов с лимитом из настроек аккаунта
    private val maxInFlight = minOf(
        parallelRequests,
        maxOf(1, (rateLimitPerSec * requestAverageProcessingTime.toMillis() / 1000.0).toInt())
    )
    private val inFlightCount = AtomicInteger(0)

    private val client = OkHttpClient.Builder().build()

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val deadlineAt = resolveDeadline(paymentStartedAt, deadline)

        // // кейс 2. Честный бюджет времени ожидания слота исходя из дедлайна клиента
        // val acquireTimeoutMs = (deadlineAt - now() - expectedProcessingMillis).coerceAtLeast(0L)

        // if (acquireTimeoutMs <= 0 || !window.acquire(Duration.ofMillis(acquireTimeoutMs))) {
        //     rejectBeforeSubmission(paymentId, paymentStartedAt, deadline, deadlineAt)
        //     return
        // }

        // val slotReleased = AtomicBoolean(false)

        val transactionId = UUID.randomUUID()

        try {
            // кейс 1. блокируем поток, пока не освободится слот
            rateLimiter.tickBlocking()

            // кейс 3. после ожидания лимитера проверяем, не истёк ли дедлайн
            if (!canBeProcessedBefore(deadlineAt)) {
                rejectBeforeSubmission(paymentId, paymentStartedAt, deadline, deadlineAt)
                return
            }

            logger.warn("[$accountName] Submitting payment request for payment $paymentId")

            // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
            // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
            paymentESService.update(paymentId) {
                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }

            logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

            val request = Request.Builder().run {
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                post(emptyBody)
            }.build()

            val call = client.newCall(request)
            paymentFlowMetrics.sent()
            val response = try {
                call.execute()
            } catch (e: Exception) {
                paymentFlowMetrics.completed()
                throw e
            }
            response.use { response ->
                val body = try {
                    mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(),false, e.message)
                } finally {
                    paymentFlowMetrics.completed()
                }

                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }
            }
        } catch (e: Exception) {
            when (e) {
                is SocketTimeoutException -> {
                    logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                    }
                }

                else -> {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                }
            }
        } finally {
            // Слот окна освобождаем на всех путях (успех / отказ / исключение)
            inFlightCount.decrementAndGet()
        }
    }

    private fun canBeProcessedBefore(deadlineAt: Long) = now() + expectedProcessingMillis <= deadlineAt

    private fun resolveDeadline(paymentStartedAt: Long, deadline: Long): Long {
        if (deadline <= 0L) return Long.MAX_VALUE
        return if (deadline > paymentStartedAt) deadline else paymentStartedAt + deadline
    }

    // Оплату, которая не успеет до дедлайна, обрабатываем локально: провайдера не беспокоим, но исход фиксируем
    // в event store, чтобы сервис тестирования увидел завершённый жизненный цикл оплаты
    private fun rejectBeforeSubmission(paymentId: UUID, paymentStartedAt: Long, deadline: Long, deadlineAt: Long) {
        paymentFlowMetrics.rejectedDeadline()
        val transactionId = UUID.randomUUID()
        logger.warn(
            "[$accountName] Payment $paymentId rejected locally: deadline exceeded " +
                "(deadline=$deadline, deadlineAt=$deadlineAt, now=${now()}, expectedProcessingMillis=$expectedProcessingMillis)"
        )
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }
        paymentESService.update(paymentId) {
            it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded before submission.")
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    override fun canAcceptPayment(): Boolean {
        if (!isEnabled()) return false
        while (true) {
            val current = inFlightCount.get()
            if (current >= maxInFlight) {
                return false
            }
            if (inFlightCount.compareAndSet(current, current + 1)) {
                return true
            }
        }
    }
}

public fun now() = System.currentTimeMillis()