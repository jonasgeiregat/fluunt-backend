package kafmin.message

import io.micronaut.core.convert.ConversionService
import io.micronaut.core.convert.TypeConverterRegistrar
import io.micronaut.http.HttpResponse
import io.micronaut.http.annotation.*
import kafmin.kafka.InboundMessage
import javax.inject.Singleton

@Controller("/topics/{name}")
class MessageController(
    private val viewMessagesUseCase: ViewMessagesUseCase,
    private val publishMessageUseCase: PublishMessageUseCase
) {

    @Get("/messages")
    fun getMessages(
        @PathVariable("name") topic: String,
        @QueryValue partitions: Partitions,
        @QueryValue("message-count", defaultValue = "100") messageCount: Int
    ): MutableList<InboundMessage> {
        return viewMessagesUseCase.viewMessages(
            ViewMessagesByPartitionsDetails(
                topic,
                partitions.numbers,
                messageCount
            )
        )
            .toCollection(mutableListOf())
    }

    @Post("/messages")
    fun publishMessage(
        @PathVariable("name") topic: String,
        @Body message: OutboundMessageResource
    ) : HttpResponse<Any> {
        publishMessageUseCase.publish(MessageDetails(topic, message.key, message.body))
        return HttpResponse.accepted()
    }
}

data class OutboundMessageResource(val key: String, val body: String)

data class Partitions(private val partitionsValue: String) {

    val numbers: List<Int>

    init {
        this.numbers = partitionsValue.split(",")
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