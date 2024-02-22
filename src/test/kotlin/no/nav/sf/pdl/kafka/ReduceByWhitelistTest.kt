package no.nav.sf.pdl.kafka

import com.google.gson.GsonBuilder
import com.google.gson.JsonObject
import com.google.gson.JsonParser.parseString
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class ReduceByWhitelistTest {
    private val exampleWithSalesforceTag = readResourceFile("/exampleWithSalesforceTag.json").asRecordValue()
    private val exampleTombstone = null.asRecordValue()

    private fun String?.asRecordValue() = ConsumerRecord("topic", 0, 0L, "key", this)

    private fun String.toPrettyFormat(): String {
        val json: JsonObject = parseString(this).asJsonObject
        val gson = GsonBuilder().setPrettyPrinting().serializeNulls().create()
        return gson.toJson(json)
    }

    @Test
    fun make_sure_we_let_null_value_through() {
        val whitelist = """
            {
              "hentPerson": {
                "foedsel": "ALL"
              }
            }
        """
        assertEquals(null, reduceByWhitelist(exampleTombstone, whitelist))
    }

    @Test
    fun make_sure_filter_on_hentPerson_level_only_returns_desired_fields() {
        val whitelist = """
            {
              "hentPerson": {
                "foedsel": "ALL"
              }
            }
        """

        assertEquals(
            """
            {
              "hentPerson": {
                "foedsel": [
                  {
                    "foedselsaar": 1972,
                    "foedselsdato": "1972-03-07",
                    "foedeland": "SGP",
                    "foedested": "Fødested i/på SINGAPORE",
                    "foedekommune": null,
                    "metadata": {
                      "opplysningsId": "dcf8f038-01c4-4d5d-b2ba-787e6bdf05ed",
                      "master": "FREG",
                      "endringer": [
                        {
                          "type": "OPPRETT",
                          "registrert": "2022-01-14T15:41:32.752",
                          "registrertAv": "Folkeregisteret",
                          "systemkilde": "FREG",
                          "kilde": "Dolly"
                        }
                      ],
                      "historisk": false
                    },
                    "folkeregistermetadata": {
                      "ajourholdstidspunkt": "2022-01-14T15:41:32.752",
                      "gyldighetstidspunkt": "2022-01-14T15:41:32.752",
                      "opphoerstidspunkt": null,
                      "kilde": "Dolly",
                      "aarsak": null,
                      "sekvens": null
                    }
                  }
                ]
              }
            }
            """.trimIndent(),
            reduceByWhitelist(exampleWithSalesforceTag, whitelist)?.toPrettyFormat()
        )
    }

    @Test
    fun make_sure_filter_on_nested_levels_returns_desired_fields() {
        val whitelist = """
            {
              "hentPerson": {
                  "bostedsadresse": {
                    "vegadresse" : {
                        "husnummer": "ALL",
                        "husbokstav": "ALL"
                    }
                  },
                  "foedsel": {
                    "metadata": {
                        "master": "ALL"
                    }
                  }
              }
            }
        """

        assertEquals(
            """
            {
              "hentPerson": {
                "bostedsadresse": [
                  {
                    "vegadresse": {
                      "husnummer": "7",
                      "husbokstav": "A"
                    }
                  }
                ],
                "foedsel": [
                  {
                    "metadata": {
                      "master": "FREG"
                    }
                  }
                ]
              }
            }
            """.trimIndent(),
            reduceByWhitelist(exampleWithSalesforceTag, whitelist)?.toPrettyFormat()
        )
    }

    @Test
    fun make_sure_filter_on_nested_level_returns_desired_fields_of_multiple_objects() {
        val whitelist = """
            {
              "hentIdenter": {
                  "identer": {
                    "ident" : "ALL"
                  }
              }
            }
        """

        assertEquals(
            """
            {
              "hentIdenter": {
                "identer": [
                  {
                    "ident": "07037211162"
                  },
                  {
                    "ident": "2388995573463"
                  }
                ]
              }
            }
            """.trimIndent(),
            reduceByWhitelist(exampleWithSalesforceTag, whitelist)?.toPrettyFormat()
        )
    }
}
