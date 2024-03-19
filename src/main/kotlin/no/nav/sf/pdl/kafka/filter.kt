package no.nav.sf.pdl.kafka

import com.google.gson.JsonArray
import com.google.gson.JsonNull
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.io.File

fun isTombstoneOrSalesforceTagged(record: ConsumerRecord<String, String?>): Boolean {
    try {
        if (record.value() == null) return true // Allow tombstone signal
        val obj = JsonParser.parseString(record.value()) as JsonObject
        if (obj["tags"] == null || obj["tags"] is JsonNull) return false
        return (obj["tags"] as JsonArray).any { it.asString == "SALESFORCE" }
    } catch (e: Exception) {
        File("/tmp/filterSalesforceTagFail").appendText("$record\nMESSAGE ${e.message}\n\n")
        throw RuntimeException("Unable to parse value for salesforce tag filter ${e.message}")
    }
}

fun cherryPickedOffsets(record: ConsumerRecord<String, String?>): Boolean {
    try {
        return offsetsPersonTestData.contains(record.offset()).also {
            if (it) {
                File("/tmp/cherry-keys").appendText(record.key() + "\n")
            }
        }
    } catch (e: Exception) {
        File("/tmp/cherryPickedOffsetsFail").appendText("$record\nMESSAGE ${e.message}\n\n")
        throw RuntimeException("Unable to parse value for cherry pick filter ${e.message}")
    }
}

fun cherryPickedKeys(record: ConsumerRecord<String, String?>): Boolean {
    try {
        return keysPersonTestData.contains(record.key())
    } catch (e: Exception) {
        File("/tmp/cherryPickedKeysFail").appendText("$record\nMESSAGE ${e.message}\n\n")
        throw RuntimeException("Unable to parse value for cherry pick filter ${e.message}")
    }
}

// TEST VARIANT:
val devPersonsKeysOffsets: MutableMap<String, Long> = mutableMapOf()

val firstOffset = 20776583L
val lastOffset = 21067981L
fun isTombstoneOrSalesforceTaggedMod(record: ConsumerRecord<String, String?>): Boolean {
    try {
        if ((record.offset() < firstOffset) || (record.offset() > lastOffset)) return false
        if (record.value() == null) return true.also { updatePassedFile(record) } // Allow tombstone signal
        val obj = JsonParser.parseString(record.value()) as JsonObject
        if (obj["tags"] == null || obj["tags"] is JsonNull) return false
        return (obj["tags"] as JsonArray).any { it.asString == "SALESFORCE" }.also {
            if (it) {
                updatePassedFile(record)
            }
        }
    } catch (e: Exception) {
        File("/tmp/filterSalesforceTagFail").appendText("$record\nMESSAGE ${e.message}\n\n")
        throw RuntimeException("Unable to parse value for salesforce tag filter ${e.message}")
    }
}

fun updatePassedFile(record: ConsumerRecord<String, String?>) {
    val tombstone = record.value() == null
    val latestRefOffset = devPersonsKeysOffsets[record.key()]?.toString() ?: ""
    File("/tmp/passed").appendText("${record.offset()} ${if (tombstone) "TOMBSTONE" else "PERSON"} LATEST $latestRefOffset\n")
    if (!tombstone) devPersonsKeysOffsets[record.key()] = record.offset()
}

val offsetsPersonTestData = listOf(
    20776583L,
    20777320L,
    20785633L,
    20791528L,
    20791653L,
    20791780L,
    20791851L,
    20791869L,
    20791948L,
    20791972L,
    20792284L,
    20794396L,
    20795731L,
    20796087L,
    20796226L,
    20796442L,
    20796597L,
    20803973L,
    20804377L,
    20804937L,
    20819196L,
    20820591L,
    20821610L,
    20826206L,
    20826988L,
    20834144L,
    20834405L,
    20843818L,
    20845403L,
    20846677L,
    20851973L,
    20854489L,
    20867468L,
    20868082L,
    20875773L,
    20877195L,
    20889203L,
    20889831L,
    20900469L,
    20906387L,
    20910143L,
    20912647L,
    20920861L,
    20922457L,
    20923558L,
    20924940L,
    20926176L,
    20931430L,
    20937347L,
    20943992L,
    20947098L,
    20954833L,
    20962435L,
    20972693L,
    20977380L,
    20977616L,
    20983036L,
    20983400L,
    20983526L,
    20987700L,
    20987873L,
    20989490L,
    20996327L,
    20996764L,
    20997150L,
    20998857L,
    21002595L,
    21013803L,
    21023757L,
    21030623L,
    21031646L,
    21033214L,
    21035618L,
    21036290L,
    21040943L,
    21042028L,
    21058287L,
    21067977L,
    21067979L,
    21067981L
)

val keysPersonTestData = listOf(
    "2099908885003",
    "2499033555167",
    "2756632594775",
    "2060252018637",
    "2694423847869",
    "2449194246205",
    "2634589628285",
    "2337616066406",
    "2614090183199",
    "2023705212058",
    "2092852764905",
    "2204689106791",
    "2593216617320",
    "2345285010543",
    "2254033892901",
    "2625259744768",
    "2769966317332",
    "2216083988788",
    "2259231953930",
    "2635335799342",
    "2277064427345",
    "2141000920230",
    "2670565513968",
    "2624643866340",
    "2027128695524",
    "2665445435234",
    "2849050616568",
    "2270361426621",
    "2996479580701",
    "2438062547468",
    "2966403775687",
    "2826966101108",
    "2520772282785",
    "2647108982315",
    "2030005907594",
    "2854231778503",
    "2862322179123",
    "2142978216791",
    "2574949056592",
    "2925035527769",
    "2575326576004",
    "2941150890077",
    "2038396981299",
    "2699778029826",
    "2773922120592",
    "2293540886648",
    "2681085940649",
    "2982967564809",
    "2053678734606",
    "2068841071646",
    "2778718296436",
    "2469949510940",
    "2490602142528",
    "2962986030108",
    "2230768234851",
    "2161076437887",
    "2839700703717",
    "2724162062365",
    "2361956210498",
    "2846303922519",
    "2666054071261",
    "2985974944638",
    "2626498477331",
    "2228014503427",
    "2248698609752",
    "2048519991961",
    "2951228317600",
    "2827927821956",
    "2098288899456",
    "2730928226752",
    "2803197856315",
    "2320553995605",
    "2101029311073",
    "2132836219686",
    "2151647410388",
    "2985280386383",
    "2935337736148",
    "2935337736148",
    "2935337736148"
)
