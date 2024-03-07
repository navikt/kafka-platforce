package no.nav.sf.pdl.kafka.salesforce

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import mu.KotlinLogging
import org.http4k.core.Response
import org.http4k.core.Status
import java.lang.reflect.Type

/**
 * Please refer to
 * https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_composite_sobjects_collections_create.htm
 */

private val log = KotlinLogging.logger { }

private val gson = Gson()

/**
 * The general sObject REST API for posting records of different types
 * In this case, post of KafkaMessage containing attribute refering to Salesforce custom object KafkaMessage__c
 */
data class SFsObjectRest(
    val allOrNone: Boolean = true,
    val records: Set<KafkaMessage>
) {
    fun toJson(): String = gson.toJson(this)
}

data class KafkaMessage(
    val attributes: SFsObjectRestAttributes = SFsObjectRestAttributes(),
    val CRM_Topic__c: String,
    val CRM_Key__c: String,
    val CRM_Value__c: String?
)

data class SFsObjectRestAttributes(
    val type: String = "KafkaMessage__c"
)

data class SFsObjectStatus(
    val id: String = "",
    val success: Boolean,
    val errors: List<SFObjectError> = emptyList()
)

data class SFObjectError(
    val statusCode: String,
    val message: String,
    val fields: List<String> = emptyList()
)

fun Response.isSuccess(): Boolean = when (status) {
    Status.OK ->
        try {
            val listOfStatusObject: Type = object : TypeToken<ArrayList<SFsObjectStatus>>() {}.type
            val parsedResult = Gson().fromJson(bodyString(), listOfStatusObject) as List<SFsObjectStatus>
            // Salesforce gives 200 OK independent of successful posting of records or not, need to check response value
            if (parsedResult.isEmpty()) {
                log.error { "Posting response has no status object successes" }
                false
            } else if (parsedResult.all { it.success }) {
                true
            } else {
                log.error { "Posting of at least one record failed" }
                false
            }
        } catch (e: Exception) {
            log.error { "Post status parse error" }
            false
        }
    else -> {
        log.error { "Post request to Salesforce failed - ${status.description}(${status.code})" }
        false
    }
}
