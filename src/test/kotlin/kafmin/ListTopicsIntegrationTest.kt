package kafmin

import io.restassured.http.ContentType
import kafmin.topic.TopicResource
import org.hamcrest.Matchers
import org.junit.jupiter.api.Test

class ListTopicsIntegrationTest : BaseTest() {

    @Test
    fun shouldListTopics() {
        request.body(TopicResource("_list.topic.1"))
            .contentType(ContentType.JSON)
            .post("/topics")
            .then()
            .assertThat()
            .statusCode(202)

        request.contentType(ContentType.JSON)
            .get("/topics")
            .then()
            .assertThat()
            .statusCode(200)
            .body("find { it.name.startsWith('_') }", Matchers.nullValue())
    }

    @Test
    fun shouldIgnoreInternalTopicsWhenQueryParamIsFalse() {
        request.body(TopicResource("_list.topic.3"))
            .contentType(ContentType.JSON)
            .post("/topics")
            .then()
            .assertThat()
            .statusCode(202)

        request.contentType(ContentType.JSON)
            .get("/topics?internal=false")
            .then()
            .assertThat()
            .statusCode(200)
            .body("find { it.name.startsWith('_') }", Matchers.nullValue())
    }

    @Test
    fun shouldListInternalTopics() {
        request.body(TopicResource("_list.topic.2"))
            .contentType(ContentType.JSON)
            .post("/topics?internal=true")
            .then()
            .assertThat()
            .statusCode(202)

        request.contentType(ContentType.JSON)
            .get("/topics?internal=true")
            .then()
            .assertThat()
            .statusCode(200)
            .body("find { it.name.startsWith('_') }", Matchers.nullValue())
    }
}