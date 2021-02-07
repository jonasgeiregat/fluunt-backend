package kafmin

import io.restassured.http.ContentType
import kafmin.message.OutboundMessageResource
import kafmin.topic.TopicResource
import org.junit.jupiter.api.Test


class MessagePublicationIntegrationTest : BaseTest() {

    @Test
    fun shouldRetrieveSimpleStringMessage() {
//        val props = Properties()
//        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
//        props.put(
//            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
//            StringSerializer::class.java
//        )
//        props.put(
//            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
//            KafkaAvroSerializer::class.java
//        )
//        props.put("schema.registry.url", "http://localhost:8081")
//        val producer: KafkaProducer<Any, Any> = KafkaProducer<Any, Any>(props)
//
//        val key = "key1"
//        val userSchema = "{\"type\":\"record\"," +
//                "\"name\":\"myrecord\"," +
//                "\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}]}"
//        val parser: Schema.Parser = Schema.Parser()
//        val schema: Schema = parser.parse(userSchema)
//        val avroRecord: GenericRecord = GenericData.Record(schema)
//        avroRecord.put("f1", "value1")
//
//        val record = ProducerRecord<Any, Any>("topic2", key, avroRecord)
//        try {
//            val x = producer.send(record).get()
//            println(x)
//        } catch (e: SerializationException) {
//            // may need to do something with it
//            val i = 1;
//        } // When you're finished producing records, you can flush the producer to ensure it has all been written to Kafka and
//        // then close the producer to free its resources.
//        finally {
//            producer.flush()
//            producer.close()
//        }


        request.body(TopicResource("message.publication.test"))
            .contentType(ContentType.JSON)
            .post("/topics")
            .then()
            .assertThat()
            .statusCode(202)
        request.body(OutboundMessageResource("key", "value"))
            .contentType(ContentType.JSON)
            .post("/topics/message.publication.test/messages")
            .then()
            .assertThat()
            .statusCode(202)
        request.contentType(ContentType.JSON)
            .get("/topics/message.publication.test/messages?partitions=0")
            .then()
            .assertThat()
            .statusCode(200)
    }
}