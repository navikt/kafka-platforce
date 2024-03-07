package no.nav.sf.pdl.kafka.poster

import com.google.gson.Gson
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import no.nav.sf.pdl.kafka.kafka.KafkaConsumerFactory
import no.nav.sf.pdl.kafka.readResourceFile
import no.nav.sf.pdl.kafka.salesforce.KafkaMessage
import no.nav.sf.pdl.kafka.salesforce.SFsObjectStatus
import no.nav.sf.pdl.kafka.salesforce.SalesforceClient
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.TopicPartition
import org.http4k.core.Response
import org.http4k.core.Status
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.Base64

class KafkaToSFPosterTest {
    private val exampleWithSalesforceTagRecord = readResourceFile("/exampleWithSalesforceTag.json").asRecordValue()
    private val exampleWithSalesforceTagWithOffset1Record = readResourceFile("/exampleWithSalesforceTag.json").asRecordValue(2L)

    private fun String?.asRecordValue(offset: Long = 1L) = ConsumerRecord("topic", 0, offset, "key", this)

    // Helper function to create an instance of ConsumerRecords from list of consumer records
    private fun List<ConsumerRecord<String, String?>>.toConsumerRecords(): ConsumerRecords<String, String?> {
        return ConsumerRecords<String, String?>(mapOf(TopicPartition("topic", 0) to this))
    }

    private val gson = Gson()

    private val sfClientMock = mockk<SalesforceClient>()
    private val kafkaConsumerFactoryMock = mockk<KafkaConsumerFactory>()
    private val kafkaConsumerMock = mockk<KafkaConsumer<String, String?>>()
    private val partitionInfoMock = mockk<PartitionInfo>()

    val filterMock: (ConsumerRecord<String, String?>) -> Boolean = mockk()

    // To be tested:
    lateinit var kafkaToSFPoster: KafkaToSFPoster

    // Parameters to be altered by test cases
    var flags: List<KafkaToSFPoster.Flag> = listOf()
    var filter: ((ConsumerRecord<String, String?>) -> Boolean)? = null
    var modifier: ((ConsumerRecord<String, String?>) -> String?)? = null

    @BeforeEach
    fun setUp() {
        every { kafkaConsumerFactoryMock.createConsumer() } returns kafkaConsumerMock
        every { kafkaConsumerMock.partitionsFor(any()) } returns listOf(partitionInfoMock)
        every { partitionInfoMock.topic() } returns "topic"
        every { partitionInfoMock.partition() } returns 0
        every { kafkaConsumerMock.assign(any()) } returns Unit

        every { sfClientMock.postRecords(any()) } returns Response(Status.OK).body("[${gson.toJson(SFsObjectStatus("id", true))}]")
        every { kafkaConsumerMock.commitSync() } returns Unit

        kafkaToSFPoster = KafkaToSFPoster(
            filter = filter,
            modifier = modifier,
            sfClient = sfClientMock,
            kafkaTopic = "topic",
            kafkaConsumerFactory = kafkaConsumerFactoryMock,
            kafkaPollDuration = 10L,
            flags = flags
        )
    }

    @AfterEach
    fun tearDown() {
        flags = listOf()
        filter = null
        modifier = null
    }

    @Test
    fun `Two polls - when consumer pulls one record on two subsequent poll calls and none on a third call that work session should result in two post calls to salesforce`() {
        // Setup mock responses to poll:
        val pollResponse1 = listOf(exampleWithSalesforceTagRecord).toConsumerRecords() // Result of first poll call - one record
        val pollResponse2 = listOf(exampleWithSalesforceTagWithOffset1Record).toConsumerRecords() // Result of second poll call - one record
        val pollResponse3 = ConsumerRecords<String, String?>(mapOf()) // Result of third poll call - no records

        every { kafkaConsumerMock.poll(any<Duration>()) } returnsMany listOf(pollResponse1, pollResponse2, pollResponse3)

        kafkaToSFPoster.runWorkSession()

        verify(exactly = 2) {
            sfClientMock.postRecords(
                setOf(
                    KafkaMessage(
                        CRM_Topic__c = "topic", CRM_Key__c = "key",
                        CRM_Value__c = Base64.getEncoder().encodeToString(readResourceFile("/exampleWithSalesforceTag.json").toByteArray())
                    )
                )
            )
        }
        verify { kafkaConsumerMock.commitSync() }
    }

    @Test
    fun `Duplicate removed - when consumer pulls two records with same key and value on the same poll call that work session should result in one post call to salesforce without the duplicate`() {
        val pollResponse1 = listOf(exampleWithSalesforceTagRecord, exampleWithSalesforceTagWithOffset1Record).toConsumerRecords() // 2 records, one duplicate except offset
        val pollResponse2 = ConsumerRecords<String, String?>(mapOf())

        every { kafkaConsumerMock.poll(any<Duration>()) } returnsMany listOf(pollResponse1, pollResponse2)

        kafkaToSFPoster.runWorkSession()

        verify(exactly = 1) {
            sfClientMock.postRecords(
                setOf(
                    KafkaMessage(
                        CRM_Topic__c = "topic", CRM_Key__c = "key",
                        CRM_Value__c = Base64.getEncoder().encodeToString(readResourceFile("/exampleWithSalesforceTag.json").toByteArray())
                    )
                )
            )
        }
        verify { kafkaConsumerMock.commitSync() }
    }

    @Test
    fun `Run once flag - if first work session finds no records but later would - with RUN_ONCE flag we should only poll once and never commit`() {
        flags = listOf(KafkaToSFPoster.Flag.RUN_ONCE)
        setUp()

        val pollResponse1 = ConsumerRecords<String, String?>(mapOf()) // Result of first poll call - no records - should end first work session
        val pollResponse2 = listOf(exampleWithSalesforceTagRecord).toConsumerRecords() // Result of a second poll call - one record
        val pollResponse3 = ConsumerRecords<String, String?>(mapOf()) // Result of a third poll call - no more records

        every { kafkaConsumerMock.poll(any<Duration>()) } returnsMany listOf(pollResponse1, pollResponse2, pollResponse3)

        kafkaToSFPoster.runWorkSession()
        kafkaToSFPoster.runWorkSession()
        kafkaToSFPoster.runWorkSession()

        verify(exactly = 1) { kafkaConsumerMock.poll(any<Duration>()) } // Only first work session results in a poll
        verify(exactly = 0) { kafkaConsumerMock.commitSync() }
    }

    @Test
    fun `From beginning flag - should make consumer seek to beginning on first work session`() {
        flags = listOf(KafkaToSFPoster.Flag.FROM_BEGINNING)
        setUp()

        val pollResponse1 = ConsumerRecords<String, String?>(mapOf()) // Result of first poll call - no records

        every { kafkaConsumerMock.seekToBeginning(any()) } returns Unit
        every { kafkaConsumerMock.poll(any<Duration>()) } returns pollResponse1

        kafkaToSFPoster.runWorkSession()

        verify(exactly = 1) { kafkaConsumerMock.seekToBeginning(any()) }
    }

    @Test
    fun `Flag no post - should not post anything but still commit when there is a record from poll`() {
        flags = listOf(KafkaToSFPoster.Flag.NO_POST)
        setUp()

        val pollResponse1 = listOf(exampleWithSalesforceTagRecord).toConsumerRecords() // Result of first poll call
        val pollResponse2 = ConsumerRecords<String, String?>(mapOf()) // Result of second poll call - no records

        every { kafkaConsumerMock.poll(any<Duration>()) } returnsMany listOf(pollResponse1, pollResponse2)

        kafkaToSFPoster.runWorkSession()

        verify(exactly = 0) { sfClientMock.postRecords(any()) }
        verify { kafkaConsumerMock.commitSync() }
    }

    @Test
    fun `Filter - Like Two polls test case with a filter that lets everything through should result in the same postRecord calls`() {
        filter = filterMock
        setUp()

        // Apply a filter that lets all records through
        every { filterMock(any()) } returns true

        // Setup mock responses to poll:
        val pollResponse1 = listOf(exampleWithSalesforceTagRecord).toConsumerRecords() // Result of first poll call - one record
        val pollResponse2 = listOf(exampleWithSalesforceTagWithOffset1Record).toConsumerRecords() // Result of second poll call - one record
        val pollResponse3 = ConsumerRecords<String, String?>(mapOf()) // Result of third poll call - no records

        every { kafkaConsumerMock.poll(any<Duration>()) } returnsMany listOf(pollResponse1, pollResponse2, pollResponse3)

        kafkaToSFPoster.runWorkSession()

        verify(exactly = 2) { filterMock(any()) }
        verify(exactly = 2) {
            sfClientMock.postRecords(
                setOf(
                    KafkaMessage(
                        CRM_Topic__c = "topic", CRM_Key__c = "key",
                        CRM_Value__c = Base64.getEncoder().encodeToString(readResourceFile("/exampleWithSalesforceTag.json").toByteArray())
                    )
                )
            )
        }
        verify { kafkaConsumerMock.commitSync() }
    }

    @Test
    fun `Filter - Like Two polls test case with a filter that lets nothing through should result in no postRecord calls`() {
        filter = filterMock
        setUp()

        // Apply a filter that lets nothing through
        every { filterMock(any()) } returns false

        // Setup mock responses to poll:
        val pollResponse1 = listOf(exampleWithSalesforceTagRecord).toConsumerRecords() // Result of first poll call - one record
        val pollResponse2 = listOf(exampleWithSalesforceTagWithOffset1Record).toConsumerRecords() // Result of second poll call - one record
        val pollResponse3 = ConsumerRecords<String, String?>(mapOf()) // Result of third poll call - no records

        every { kafkaConsumerMock.poll(any<Duration>()) } returnsMany listOf(pollResponse1, pollResponse2, pollResponse3)

        kafkaToSFPoster.runWorkSession()

        verify(exactly = 2) { filterMock(any()) }
        verify(exactly = 0) {
            sfClientMock.postRecords(
                setOf(
                    KafkaMessage(
                        CRM_Topic__c = "topic", CRM_Key__c = "key",
                        CRM_Value__c = Base64.getEncoder().encodeToString(readResourceFile("/exampleWithSalesforceTag.json").toByteArray())
                    )
                )
            )
        }
        verify(exactly = 2) { kafkaConsumerMock.commitSync() }
    }

    @Test
    fun `Modifier - Like Two polls test case with a modifier - should result in the same postRecord calls with the modification applied to record values`() {
        modifier = { it.value().toString() + "-modification" }
        Assertions.assertEquals("value-modification", modifier!!("value".asRecordValue()))
        setUp()

        // Apply a filter that lets all records through
        every { filterMock(any()) } returns true

        // Setup mock responses to poll:
        val pollResponse1 = listOf(exampleWithSalesforceTagRecord).toConsumerRecords() // Result of first poll call - one record
        val pollResponse2 = listOf(exampleWithSalesforceTagWithOffset1Record).toConsumerRecords() // Result of second poll call - one record
        val pollResponse3 = ConsumerRecords<String, String?>(mapOf()) // Result of third poll call - no records

        every { kafkaConsumerMock.poll(any<Duration>()) } returnsMany listOf(pollResponse1, pollResponse2, pollResponse3)

        kafkaToSFPoster.runWorkSession()

        verify(exactly = 2) {
            sfClientMock.postRecords(
                setOf(
                    KafkaMessage(
                        CRM_Topic__c = "topic", CRM_Key__c = "key",
                        CRM_Value__c = Base64.getEncoder().encodeToString((readResourceFile("/exampleWithSalesforceTag.json") + "-modification").toByteArray())
                    )
                )
            )
        }
        verify { kafkaConsumerMock.commitSync() }
    }
}
