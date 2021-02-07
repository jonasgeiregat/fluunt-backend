package kafmin.topic

import io.micronaut.core.annotation.Introspected
import io.micronaut.http.HttpResponse
import io.micronaut.http.annotation.*
import kafmin.Failed
import kafmin.Succeeded
import kafmin.kafka.Topic
import java.net.URI

@Controller("/topics")
@SuppressWarnings("unused")
class TopicsController(
    private val listTopicsUseCase: ListTopicsUseCase,
    private val createTopicUseCase: CreateTopicUseCase
) {

    @Get
    fun getTopics(
        @QueryValue("internal", defaultValue = "false") internal: Boolean
    ): List<Topic> =
        if (internal) listTopicsUseCase.listIncludingInternalTopics() else listTopicsUseCase.listTopics()

    @Post
    fun createTopic(@Body topicResource: TopicResource): HttpResponse<Any> {
        if (topicResource.name == null) {
            return HttpResponse.badRequest()
        }
        return when (val creation = createTopicUseCase.createTopic(topicResource.toDetails())) {
            is Failed -> HttpResponse.badRequest(FailureResource(creation.faiure))
            is Succeeded -> HttpResponse.accepted(URI("/topics/${creation.success.name}"))
        }
    }
}

@Introspected
data class TopicResource(
    val name: String?,
    val numberOfPartitions: Int? = -1,
    val replicationFactor: Short? = -1
) {
    fun toDetails(): CreateTopicDetails {
        return CreateTopicDetails(name!!, numberOfPartitions ?: -1, replicationFactor ?: -1)
    }
}