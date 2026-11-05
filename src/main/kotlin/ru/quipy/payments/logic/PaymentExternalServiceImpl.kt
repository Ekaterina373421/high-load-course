package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.common.utils.SlidingWindowRateLimiter
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val paymentFlowMetrics: PaymentFlowMetrics,
    private val processingReserveMillis: Long,
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

    // кейс 1. создаем rate limiter с лимитом из настроек аккаунта
    private val rateLimiter = SlidingWindowRateLimiter(
        rateLimitPerSec.toLong(),
        Duration.ofMillis(SlidingWindowRateLimiter.WINDOW_MILLIS.toLong()),
    )

    // кейс 2. создаем окно in-flight запросов с лимитом из настроек аккаунта
    private val inFlightSemaphore = java.util.concurrent.Semaphore(parallelRequests, true)

    private val client = OkHttpClient.Builder().build()

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val deadlineAt = deadline

        val transactionId = UUID.randomUUID()

        try {
            if (!canBeProcessedBefore(deadlineAt)) {
                rejectBeforeSubmission(paymentId, paymentStartedAt, deadline, deadlineAt)
                return
            }

            // ожидание слота ограничено исходным дедлайном клиента.
            // Rate permission берем позднее, непосредственно перед HTTP-вызовом.
            val slotTimeoutMillis = deadlineAt - now() - processingReserveMillis
            if (slotTimeoutMillis <= 0 || !inFlightSemaphore.tryAcquire(slotTimeoutMillis, java.util.concurrent.TimeUnit.MILLISECONDS)) {
                rejectBeforeSubmission(paymentId, paymentStartedAt, deadline, deadlineAt)
                return
            }

            try {
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
            val limiterStartedAt = System.nanoTime()
            val permitted = rateLimiter.awaitPermission(deadlineAt - processingReserveMillis)
            val limiterWaitNanos = System.nanoTime() - limiterStartedAt
            val dispatchWaitMillis = (now() - paymentStartedAt).coerceAtLeast(0)
            if (!permitted) {
                paymentFlowMetrics.rejectedDeadline()
                logger.warn(
                    "[$accountName] Dispatch permission rejected: payment=$paymentId, txId=$transactionId, " +
                        "dispatchWaitMillis=$dispatchWaitMillis, " +
                        "limiterWaitMillis=${java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(limiterWaitNanos)}, " +
                        "remainingMillis=${deadlineAt - now()}, processingReserveMillis=$processingReserveMillis"
                )
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded waiting for dispatch.")
                }
                return
            }
            logger.info("[$accountName] Dispatch: payment=$paymentId, txId=$transactionId, dispatchWaitMillis=$dispatchWaitMillis, limiterWaitMillis=${java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(limiterWaitNanos)}, remainingMillis=${deadlineAt - now()}")
            paymentFlowMetrics.sent()
            val httpStartedAt = System.nanoTime()
            val body = try {
                    call.execute().use { response ->
                        val responseBody = response.body?.string()
                        try {
                            mapper.readValue(responseBody, ExternalSysResponse::class.java)
                        } catch (e: Exception) {
                            logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: $responseBody", e)
                            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                        }
                    }
                } finally {
                    val httpDurationNanos = System.nanoTime() - httpStartedAt
                    paymentFlowMetrics.completed()
                    logger.info("[$accountName] HTTP completed: payment=$paymentId, txId=$transactionId, httpDurationMillis=${java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(httpDurationNanos)}, remainingMillis=${deadlineAt - now()}")
                }

                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }
            } finally {
                // Слот in-flight освобождаем на всех путях (успех / отказ / исключение)
                inFlightSemaphore.release()
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
        }
    }

    private fun canBeProcessedBefore(deadlineAt: Long) = now() + processingReserveMillis < deadlineAt

    // Оплату, которая не успеет до дедлайна, обрабатываем локально: провайдера не беспокоим, но исход фиксируем
    // в event store, чтобы сервис тестирования увидел завершённый жизненный цикл оплаты
    private fun rejectBeforeSubmission(paymentId: UUID, paymentStartedAt: Long, deadline: Long, deadlineAt: Long) {
        paymentFlowMetrics.rejectedDeadline()
        val transactionId = UUID.randomUUID()
        logger.warn(
            "[$accountName] Payment $paymentId rejected locally: deadline exceeded " +
                    "(deadline=$deadline, deadlineAt=$deadlineAt, now=${now()}, " +
                    "dispatchWaitMillis=${(now() - paymentStartedAt).coerceAtLeast(0)}, " +
                    "remainingMillis=${deadlineAt - now()}, processingReserveMillis=$processingReserveMillis)"
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

    override fun accountProperties() = properties

    override fun processingReserveMillis() = processingReserveMillis

    // Кейс 5: входная проверка не используется для
    // допуска - её заменил ingress rate limiter в OrderPayer
    override fun canAcceptPayment(): Boolean = isEnabled()

}

public fun now() = System.currentTimeMillis()