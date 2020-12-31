package kafmin

import io.restassured.http.ContentType
import kafmin.message.OutboundMessageResource
import kafmin.topic.TopicResource
import org.junit.jupiter.api.Test

class MessagePublicationScenariosTest : BaseTest() {

    @Test
    fun shouldCreateNewTopicWithNameOnly() {
        //@formatter:off
        request.body(TopicResource("message.publication.test"))
            .contentType(ContentType.JSON)
            .post("/topics")
            .then()
            .assertThat()
            .statusCode(202)
        request.body(OutboundMessageResource( "key", "value"))
            .contentType(ContentType.JSON)
            .post("/topics/message.publication.test/messages")
            .then()
            .assertThat()
            .statusCode(202)
        //@formatter:on
    }
}