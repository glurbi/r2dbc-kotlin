package woow

import io.r2dbc.pool.ConnectionPool
import io.r2dbc.pool.ConnectionPoolConfiguration
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration
import io.r2dbc.postgresql.PostgresqlConnectionFactory
import io.r2dbc.spi.Connection
import io.r2dbc.spi.ConnectionFactory
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactive.awaitSingle
import kotlinx.coroutines.runBlocking
import java.time.Duration
import kotlin.system.measureTimeMillis

private fun createConnectionFactory(): ConnectionFactory {
    return PostgresqlConnectionFactory(
        PostgresqlConnectionConfiguration.builder()
            .host("localhost")
            .port(5433)
            .database("postgres")
            .username("postgres")
            .password("postgres")
            .build()
    )
}

private fun createConnectionPool(): ConnectionPool {
    val postgresqlConnectionFactory = createConnectionFactory()
    val poolConfiguration = ConnectionPoolConfiguration.builder()
        .connectionFactory(postgresqlConnectionFactory)
        .initialSize(10)
        .maxSize(50)
        .maxCreateConnectionTime(Duration.ofSeconds(10))
        .validationQuery("SELECT 1")
        .build()
    return ConnectionPool(poolConfiguration)
}

private suspend fun createUserTable(connection: Connection) {
    val createTableSQL = """
        CREATE TABLE IF NOT EXISTS users (
            id BIGSERIAL PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            email VARCHAR(50) NOT NULL UNIQUE
        )
    """.trimIndent()

    val statement = connection.createStatement(createTableSQL)
    statement.execute().awaitFirstOrNull()
}

fun main() = runBlocking {
    val connectionPool = createConnectionPool()
    val timeInMillis = measureTimeMillis {
        coroutineScope {
            repeat(500) { index ->
                launch {
                    val connection = connectionPool.create().awaitSingle()
                    println("Coroutine $index starting")
                    createUserTable(connection)
                    println("Coroutine $index finished")
                    connection.close().awaitFirstOrNull()
                }
            }
        }
    }
    println("Total execution time: $timeInMillis ms")
}
