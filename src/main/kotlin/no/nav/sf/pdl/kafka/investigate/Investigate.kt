package no.nav.sf.pdl.kafka.investigate

import org.http4k.core.HttpHandler
import org.http4k.core.Response
import org.http4k.core.Status

object Investigate {
    var investigateInProgress = false

    val investigateHandler: HttpHandler = {
        val ids = it.query("id")?.split(",")
        if (ids == null) {
            Response(Status.OK).body("Use query parameter id with csv of ids to search for")
        } else {
            Response(Status.OK).body("Will trigger search for these: " + ids.joinToString(" "))
        }
    }
}
