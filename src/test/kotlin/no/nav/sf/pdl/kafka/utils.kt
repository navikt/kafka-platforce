@file:Suppress("ktlint:standard:filename")

package no.nav.sf.pdl.kafka

fun readResourceFile(path: String) = KafkaPosterApplication::class.java.getResource(path).readText()
