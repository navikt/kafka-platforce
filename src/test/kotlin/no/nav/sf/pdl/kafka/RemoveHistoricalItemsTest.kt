package no.nav.sf.pdl.kafka

import com.google.gson.GsonBuilder
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class RemoveHistoricalItemsTest {

    private fun String.toPrettyFormat(): String {
        val json: JsonObject = JsonParser.parseString(this).asJsonObject
        val gson = GsonBuilder().setPrettyPrinting().serializeNulls().create()
        return gson.toJson(json)
    }

    @Test
    fun make_sure_we_let_null_value_through() {
        // Example JSON input
        val jsonString = """{
        "hentPerson": {
            "bostedsadresse": [
                {
                    "metadata": { "historisk": true }
                },
                {
                    "metadata": { "historisk": false }
                }
            ],
            "navn": [
                {
                    "fornavn": "John",
                    "metadata": { "historisk": true }
                },
                {
                    "fornavn": "Jane",
                    "metadata": { "historisk": false }
                }
            ],
            "inntekt": [
                {
                    "belop": 500000,
                    "metadata": { "historisk": true }
                },
                {
                    "belop": 600000
                },
                {
                    "belop": 700000,
                    "metadata": { "historisk": false }
                }
            ]
        }
    }"""

        // Call the function to remove historical entries
        val updatedJson = removeHistoricalItems(jsonString)

        // Print the updated JSON
        println(updatedJson.toPrettyFormat())
        Assertions.assertEquals(
            """
        {
          "hentPerson": {
            "bostedsadresse": [
              {
                "metadata": {
                  "historisk": false
                }
              }
            ],
            "navn": [
              {
                "fornavn": "Jane",
                "metadata": {
                  "historisk": false
                }
              }
            ],
            "inntekt": [
              {
                "belop": 600000
              },
              {
                "belop": 700000,
                "metadata": {
                  "historisk": false
                }
              }
            ]
          }
        }
            """.trimIndent(),
            removeHistoricalItems(jsonString).toPrettyFormat()
        )
    }
}
