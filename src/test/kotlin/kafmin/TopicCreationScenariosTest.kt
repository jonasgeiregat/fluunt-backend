package kafmin

import io.restassured.http.ContentType
import kafmin.topic.TopicResource
import org.hamcrest.Matchers.`is`
import org.hamcrest.Matchers.notNullValue
import org.junit.jupiter.api.Test


class TopicCreationScenariosTest : BaseTest() {

    @Test
    fun shouldCreateNewTopicWithNameOnly() {
        //@formatter:off
        request.body(TopicResource("new.topic.1"))
            .contentType(ContentType.JSON)
            .post("/topics")
            .then()
                .assertThat()
                    .statusCode(202)

        request.get("/topics")
            .then()
                .assertThat()
                    .statusCode(200)
                    .contentType(ContentType.JSON)
                    .body("find { it.name == 'new.topic.1' }", notNullValue())
        //@formatter:on
    }

    @Test
    fun shouldNotAllowDuplicateTopicNames() {
        //@formatter:off
        request.body(TopicResource("new.topic.2"))
            .contentType(ContentType.JSON)
            .post("/topics")
            .then()
            .assertThat()
            .statusCode(202)

        request.body(TopicResource("new.topic.2"))
            .contentType(ContentType.JSON)
            .post("/topics")
            .then()
            .assertThat()
            .statusCode(400)
            .body("code", `is`("TOPIC_ALREADY_EXISTS"))
            .body("message", `is`("topic with name new.topic.2 already exist"))
        //@formatter:on
    }

    @Test
    fun shouldCreateNewTopicWithNumberOfPartitions() {
        //@formatter:off
        request.body(TopicResource("new.topic.3", 3))
                .contentType(ContentType.JSON)
                .post("/topics")
                .then()
                    .assertThat()
                        .statusCode(202)

        request.get("/topics")
                .then()
                    .assertThat()
                        .statusCode(200)
                        .contentType(ContentType.JSON)
                        .body("find { it.name == 'new.topic.3' }.partitions.size()", `is`(3))
        //@formatter:on
    }

    @Test
    fun test() {
        //@formatter:off
        request.get("/topics/test/messages")
            .then()
            .assertThat()
            .statusCode(200)
            .contentType(ContentType.JSON)
            .body("find { it.name == 'new.topic.1' }", notNullValue())
        //@formatter:on
    }

    @Test
    fun shouldNotAllowNullAsTopicName() {
        //@formatter:off
        request.body("""{"name": null}""")
            .contentType(ContentType.JSON)
            .post("/topics")
            .then()
                .assertThat()
                    .statusCode(400)
        //@formatter:on
    }
}
