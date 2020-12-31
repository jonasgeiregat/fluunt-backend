package kafmin

import com.fasterxml.jackson.annotation.JsonAutoDetect
import com.fasterxml.jackson.annotation.PropertyAccessor
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import io.micronaut.http.client.RxHttpClient
import io.micronaut.http.client.annotation.Client
import io.micronaut.runtime.server.EmbeddedServer
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.micronaut.test.support.TestPropertyProvider
import io.restassured.RestAssured
import io.restassured.config.ObjectMapperConfig
import io.restassured.filter.log.RequestLoggingFilter
import io.restassured.filter.log.ResponseLoggingFilter
import io.restassured.internal.mapping.Jackson2Mapper
import io.restassured.path.json.mapper.factory.Jackson2ObjectMapperFactory
import io.restassured.specification.RequestSpecification
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.TestInstance
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName
import java.lang.String.valueOf
import java.lang.reflect.Type
import javax.inject.Inject


@MicronautTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
open class BaseTest : TestPropertyProvider {

    internal lateinit var request: RequestSpecification

    private lateinit var kafka: KafkaContainer

    @Inject
    @field:Client("/")
    lateinit var client: RxHttpClient

    @Inject
    lateinit var embeddedServer: EmbeddedServer;

    @BeforeEach
    fun beforeEach() {
        val mapper = Jackson2Mapper(Jackson2ObjectMapperFactory { _, _ ->
            val mapper: ObjectMapper = ObjectMapper()
            mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY)
        })
        val config = RestAssured.config().objectMapperConfig(ObjectMapperConfig.objectMapperConfig().defaultObjectMapper(mapper))
        this.request = RestAssured.given()
                .config(config)
                .port(embeddedServer.port)
    }

    override fun getProperties(): MutableMap<String, String> {
        kafka = KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.4.3"))
                .apply { start() }
        return mutableMapOf("kafka.port" to valueOf(kafka.firstMappedPort))
    }

    @BeforeAll
    fun beforeAll() {
        RestAssured.filters(RequestLoggingFilter(), ResponseLoggingFilter())
    }

    @AfterAll
    fun afterAll() {
        kafka.close()
    }

}