package kafmin

import io.restassured.http.ContentType
import kafmin.message.OutboundMessageResource
import kafmin.topic.TopicResource
import org.hamcrest.Matchers
import org.junit.jupiter.api.Test

class MessageRetrievalIntegrationTest: BaseTest() {

    @Test
    fun shouldRetrieveSimpleMessages() {
        request.body(TopicResource("message.retrieval.test.1"))
            .contentType(ContentType.JSON)
            .post("/topics")
            .then()
            .assertThat()
            .statusCode(202)
        request.body(OutboundMessageResource("key", "value"))
            .contentType(ContentType.JSON)
            .post("/topics/message.retrieval.test.1/messages")
            .then()
            .assertThat()
            .statusCode(202)
        request.contentType(ContentType.JSON)
            .get("/topics/message.retrieval.test.1/messages")
            .then()
            .assertThat()
            .statusCode(200)
            .body("find { it.key == 'key' }.value", Matchers.`is`("value"))
            .body("find { it.key == 'key' }.offset", Matchers.`is`(0))
            .body("find { it.key == 'key' }.partition", Matchers.`is`(0))
    }

    @Test
    fun shouldRetrieveFromSpecifiedPartition() {
        request.body(TopicResource("message.retrieval.test.2", 3))
            .contentType(ContentType.JSON)
            .post("/topics")
            .then()
            .assertThat()
            .statusCode(202)
        request.body(OutboundMessageResource("key.partition.0", "value.partition.0", 0))
            .contentType(ContentType.JSON)
            .post("/topics/message.retrieval.test.2/messages")
            .then()
            .assertThat()
            .statusCode(202)
        request.body(OutboundMessageResource("key.partition.1", "value.partition.1", 1))
            .contentType(ContentType.JSON)
            .post("/topics/message.retrieval.test.2/messages")
            .then()
            .assertThat()
            .statusCode(202)
        request.body(OutboundMessageResource("key.partition.2", "value.partition.2", 2))
            .contentType(ContentType.JSON)
            .post("/topics/message.retrieval.test.2/messages")
            .then()
            .assertThat()
            .statusCode(202)
        request.contentType(ContentType.JSON)
            .get("/topics/message.retrieval.test.2/messages?partitions=1")
            .then()
            .assertThat()
            .statusCode(200)
            .body("size()", Matchers.`is`(1))
            .body("find { it.key == 'key.partition.1' }.value", Matchers.`is`("value.partition.1"))
    }

    @Test
    fun shouldRetrieveFromAllPartitionsPaged() {
        request.body(TopicResource("message.retrieval.test.5", 2))
            .contentType(ContentType.JSON)
            .post("/topics")
            .then()
            .assertThat()
            .statusCode(202)
        IntRange(0, 20).forEach {
            request.body(OutboundMessageResource("key-$it", "value-$it", partition = if((it % 2) == 0) 0 else 1))
                .contentType(ContentType.JSON)
                .post("/topics/message.retrieval.test.5/messages")
                .then()
                .assertThat()
                .statusCode(202)
        }
        request.contentType(ContentType.JSON)
            .get("/topics/message.retrieval.test.5/messages?page=2&size=5")
            .then()
            .assertThat()
            .statusCode(200)
            .body("size()", Matchers.`is`(5))
    }

}