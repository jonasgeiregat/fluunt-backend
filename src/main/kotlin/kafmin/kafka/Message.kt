package kafmin.kafka

import io.micronaut.core.annotation.Introspected

@Introspected
data class RetainedMessage(val key: String?, val value: String, val partition: Int, val offset: Long)

data class OutboundMessage(
    val key: String,
    val value: String,
    val partition: Int?
)
