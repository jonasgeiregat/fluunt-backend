package kafmin.message

import io.micronaut.core.convert.ConversionService
import io.micronaut.core.convert.TypeConverterRegistrar
import io.micronaut.http.HttpResponse
import io.micronaut.http.annotation.*
import kafmin.kafka.RetainedMessage
import javax.inject.Singleton

@Controller("/topics/{name}")
class MessageController(
    private val viewMessagesUseCase: ViewMessagesUseCase,
    private val publishMessageUseCase: PublishMessageUseCase
) {

    @Get("/messages")
    fun getMessages(
        @PathVariable("name") topic: String,
        @QueryValue("size", defaultValue = "100") size: Int,
        @QueryValue("page", defaultValue = "1") pageNumber: Int,
        @QueryValue("partitions", defaultValue = "") partitions: Int?
    ): MutableList<RetainedMessage> {
        return viewMessagesUseCase.viewMessages(
            ViewMessagesDetails(
                topic,
                partitions,
                Page(pageNumber, size)
            )
        )
            .toCollection(mutableListOf())
    }

    @Post("/messages")
    fun publishMessage(
        @PathVariable("name") topic: String,
        @Body message: OutboundMessageResource
    ): HttpResponse<Any> {
        publishMessageUseCase.publish(MessageDetails(topic, message.key, message.body, message.partition))
        return HttpResponse.accepted()
    }
}

data class OutboundMessageResource(
    val key: String,
    val body: String,
    val partition: Int? = null,
    val messageType: MessageType = MessageType.STRING
)

enum class MessageType {
    STRING, JSON, AVRO
}

data class Partitions(private val partitionsValue: String) {

    val numbers: List<Int>

    init {
            this.numbers = partitionsValue.split(",")
                .filter { it.isNotBlank() }
                .map { Integer.valueOf(it) }
                .toCollection(mutableListOf())
    }
}

@Singleton
class PartitionsConverterRegistrar : TypeConverterRegistrar {
    override fun register(conversionService: ConversionService<*>?) {
        conversionService?.addConverter(String::class.java, Partitions::class.java,
            { s -> Partitions(s) })
    }

}