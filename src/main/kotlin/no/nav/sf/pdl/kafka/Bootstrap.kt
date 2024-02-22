package no.nav.sf.pdl.kafka

val application = KafkaPosterApplication(
    filter = if (devContext) ::isTombstoneOrSalesforceTagged else null,
    modifier = ::reduceByWhitelist
)

fun main() = application.start()
