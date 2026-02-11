package no.nav.sf.pdl.kafka.db

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import mu.KotlinLogging
import no.nav.sf.pdl.kafka.application
import no.nav.sf.pdl.kafka.config_DEPLOY_APP
import no.nav.sf.pdl.kafka.env
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.SqlExpressionBuilder.eq
import org.jetbrains.exposed.sql.deleteWhere
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.transactions.TransactionManager
import org.jetbrains.exposed.sql.transactions.transaction
import org.jetbrains.exposed.sql.upsert

// const val NAIS_DB_PREFIX = "NAIS_DATABASE_SF_KEYTOOL_SF_KEYTOOL_"

class PostgresDatabase(
    naisDbPrefix: String,
) {
    private val log = KotlinLogging.logger { }

    val appName = env(config_DEPLOY_APP).uppercase().replace('-', '_')
    val naisDbPrefix = "NAIS_DATABASE_${appName}_${appName}_${application.context}_JDBC_URL"

    private val dbJdbcUrl = env("NAIS_DATABASE_${appName}_${appName}_${application.context}_JDBC_URL")

    // Note: exposed Database connect prepares for connections but does not actually open connections
    // That is handled via transaction {} ensuring connections are opened and closed properly
    val database = Database.connect(HikariDataSource(hikariConfig()))

    private fun hikariConfig(): HikariConfig =
        HikariConfig().apply {
            jdbcUrl = dbJdbcUrl // "jdbc:postgresql://localhost:$dbPort/$dbName" // This is where the cloud db proxy is located in the pod
            driverClassName = "org.postgresql.Driver"
            minimumIdle = 1
            maxLifetime = 26000
            maximumPoolSize = 10
            connectionTimeout = 250
            idleTimeout = 10000
            isAutoCommit = false
            // Isolation level that ensure the same snapshot of db during one transaction:
            transactionIsolation = "TRANSACTION_REPEATABLE_READ"
        }

//    fun createCertMetadataTable(dropFirst: Boolean = false) {
//        transaction {
//            if (dropFirst) {
//                log.info { "Dropping table $CERT_METADATA" }
//                val dropStatement =
//                    TransactionManager.current().connection.prepareStatement("DROP TABLE $CERT_METADATA", false)
//                dropStatement.executeUpdate()
//                log.info { "Drop performed" }
//            }
//
//            log.info { "Creating table $CERT_METADATA" }
//            SchemaUtils.create(CertMetadataTable)
//        }
//    }
//
//    fun upsertCertMetadata(certMetadata: CertMetadata): CertMetadata? =
//        transaction {
//            CertMetadataTable.upsert(
//                keys = arrayOf(CertMetadataTable.cn), // Perform update if there is a conflict here
//            ) {
//                it[CertMetadataTable.cn] = certMetadata.cn
//                it[CertMetadataTable.expiresAt] = certMetadata.expiresAt
//                it[CertMetadataTable.sfUsername] = certMetadata.sfUsername
//                it[CertMetadataTable.sfClientId] = certMetadata.sfClientId
//            }
//        }.resultedValues?.firstOrNull()?.toCertMetadata()
//
//    fun deleteCertMetadata(cn: String) {
//        transaction {
//            CertMetadataTable.deleteWhere {
//                (CertMetadataTable.cn eq cn)
//            }
//        }
//    }
//
//    fun retrieveCertMetadata(): List<CertMetadata> =
//        transaction {
//            CertMetadataTable
//                .selectAll()
//                .map { it.toCertMetadata() }
//        }.toList()
}
