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
import java.io.InterruptedIOException
import java.io.IOException
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val paymentFlowMetrics: PaymentFlowMetrics,
    private val paymentAttemptMetrics: PaymentAttemptMetrics,
    private val processingReserveMillis: Long,
    private val attemptTimeoutMillis: Long = 0L,
    private val maxAttempts: Int = 2,
    private val retryTimeouts: Boolean = false,
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

    init {
        require(attemptTimeoutMillis >= 0)
        require(maxAttempts > 0)
        paymentAttemptMetrics.registerAccount(accountName)
        logger.info("[$accountName] Attempt policy: timeoutMillis=$attemptTimeoutMillis, maxAttempts=$maxAttempts, retryTimeouts=$retryTimeouts")
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val deadlineAt = deadline

        val transactionId = UUID.randomUUID()

        try {
            if (!canBeProcessedBefore(deadlineAt)) {
                rejectBeforeSubmission(paymentId, paymentStartedAt, deadline, deadlineAt)
                return
            }

            // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
            // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
            // Фиксируем регистрацию отправки в Event Store ровно один раз (до retry-цикла).
            paymentESService.update(paymentId) {
                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }

            logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

            val pureHttpTimeMs = requestAverageProcessingTime.toMillis().coerceAtLeast(0)

            var attempts = 0
            var finalBody: ExternalSysResponse? = null

            // новую попытку начинаем, только если осталось время на полный HTTP timeout.
            val minimumAttemptBudget = if (attemptTimeoutMillis > 0)
                attemptTimeoutMillis else pureHttpTimeMs

            while (attempts < maxAttempts) {
                val requiredReserve = if (attemptTimeoutMillis > 0) minimumAttemptBudget
                    else if (attempts == 0) processingReserveMillis else pureHttpTimeMs
                val slotTimeoutMillis = deadlineAt - now() - requiredReserve
                if (slotTimeoutMillis <= 0) break

                val acquired = inFlightSemaphore.tryAcquire(slotTimeoutMillis, TimeUnit.MILLISECONDS)
                if (!acquired) break

                try {
                    val limiterStartedAt = System.nanoTime()
                    val permitted = rateLimiter.awaitPermission(deadlineAt - minimumAttemptBudget)
                    val limiterWaitNanos = System.nanoTime() - limiterStartedAt
                    val dispatchWaitMillis = (now() - paymentStartedAt).coerceAtLeast(0)

                    if (!permitted) {
                        logger.warn(
                            "[$accountName] Dispatch permission rejected: payment=$paymentId, txId=$transactionId, " +
                                "dispatchWaitMillis=$dispatchWaitMillis, " +
                                "limiterWaitMillis=${TimeUnit.NANOSECONDS.toMillis(limiterWaitNanos)}, " +
                                "remainingMillis=${deadlineAt - now()}, processingReserveMillis=$processingReserveMillis, " +
                                "attempt=$attempts"
                        )
                        break
                    }

                    val remainingForHttp = deadlineAt - now()
                    if (remainingForHttp <= 0 || (attemptTimeoutMillis > 0 && remainingForHttp < attemptTimeoutMillis)) break
                    val httpTimeoutMillis = if (attemptTimeoutMillis > 0)
                        minOf(attemptTimeoutMillis, remainingForHttp) else remainingForHttp

                    attempts++
                    logger.info(
                        "[$accountName] Dispatch: payment=$paymentId, txId=$transactionId, " +
                            "dispatchWaitMillis=$dispatchWaitMillis, " +
                            "limiterWaitMillis=${TimeUnit.NANOSECONDS.toMillis(limiterWaitNanos)}, " +
                            "remainingMillis=${deadlineAt - now()}, attempt=$attempts"
                    )
                    paymentFlowMetrics.sent()

                    val request = Request.Builder().run {
                        url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                        post(emptyBody)
                    }.build()

                    val call = client.newCall(request)
                    call.timeout().timeout(httpTimeoutMillis, TimeUnit.MILLISECONDS)

                    val httpStartedAt = System.nanoTime()
                    var httpOutcome = "error"
                    if (attempts > 1) paymentAttemptMetrics.retryStarted(accountName)
                    val body = try {
                        call.execute().use { response ->
                            val responseBody = response.body?.string()
                            try {
                                mapper.readValue(responseBody, ExternalSysResponse::class.java).also {
                                    httpOutcome = if (response.isSuccessful && it.result) "success" else "provider_failure"
                                }
                            } catch (e: Exception) {
                                httpOutcome = "invalid_response"
                                logger.error(
                                    "[$accountName] [ERROR] Payment processed for txId: $transactionId, " +
                                        "payment: $paymentId, result code: ${response.code}, reason: $responseBody",
                                    e,
                                )
                                ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                            }
                        }
                    } catch (e: Exception) {
                        httpOutcome = when {
                            e is SocketTimeoutException -> "timeout"
                            e is InterruptedIOException && e.message?.contains("timeout", ignoreCase = true) == true -> "timeout"
                            e is IOException -> "io_error"
                            else -> "error"
                        }
                        logger.error(
                            "[$accountName] [ERROR] Payment HTTP call failed for txId: $transactionId, " +
                                "payment: $paymentId, attempt=$attempts",
                            e,
                        )
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                    } finally {
                        val httpDurationNanos = System.nanoTime() - httpStartedAt
                        paymentAttemptMetrics.recordHttp(accountName, httpOutcome, httpDurationNanos)
                        paymentFlowMetrics.completed()
                        logger.info(
                            "[$accountName] HTTP completed: payment=$paymentId, txId=$transactionId, " +
                                "httpDurationMillis=${TimeUnit.NANOSECONDS.toMillis(httpDurationNanos)}, " +
                                "remainingMillis=${deadlineAt - now()}"
                        )
                    }

                    finalBody = body
                    logger.warn(
                        "[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, " +
                            "succeeded: ${body.result}, message: ${body.message}, attempt=$attempts"
                    )

                    val isTemporaryError =
                        !body.result && body.message?.trim()?.equals("Temporary error", ignoreCase = true) == true
                    val retryableTimeout = retryTimeouts && httpOutcome == "timeout"
                    if (body.result || !(isTemporaryError || retryableTimeout)) {
                        break // Успех или неповторяемая ошибка — выходим из цикла
                    }
                    if (attempts < maxAttempts) {
                        logger.info("[$accountName] Retry candidate: payment=$paymentId, txId=$transactionId, attempt=$attempts, reason=$httpOutcome, remainingMillis=${deadlineAt - now()}")
                    }
                } finally {
                    // Слот in-flight освобождаем на всех путях (успех / отказ / исключение)
                    inFlightSemaphore.release()
                }
            }

            // Финальная фиксация результата в Event Store без искажения доменной модели
            val result = finalBody
            if (result != null) {
                // HTTP-запрос вызвался хотя бы раз: пишем реальный результат провайдера (true или false)
                paymentESService.update(paymentId) {
                    it.logProcessing(
                        result.result,
                        now(),
                        transactionId,
                        reason = result.message ?: if (result.result) "SUCCESS" else "Failed after $attempts attempt(s)",
                    )
                }
            } else {
                // До HTTP вызова дело не дошло (превышен бюджет времени/семафора/rate limiter)
                paymentFlowMetrics.rejectedDeadline()
                logger.warn(
                    "[$accountName] Payment $paymentId rejected locally: deadline exceeded " +
                        "(deadline=$deadline, deadlineAt=$deadlineAt, now=${now()}, " +
                        "dispatchWaitMillis=${(now() - paymentStartedAt).coerceAtLeast(0)}, " +
                        "remainingMillis=${deadlineAt - now()}, processingReserveMillis=$processingReserveMillis)"
                )
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded before HTTP execution.")
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