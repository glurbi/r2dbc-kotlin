package woow

import io.r2dbc.postgresql.PostgresqlConnectionConfiguration
import io.r2dbc.postgresql.PostgresqlConnectionFactory
import io.r2dbc.spi.Connection
import io.r2dbc.spi.ConnectionFactory
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactive.awaitLast
import kotlinx.coroutines.reactive.awaitSingle
import kotlinx.coroutines.runBlocking

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

private suspend fun deleteUserTable(connection: Connection) {
    val dropTableSQL = "DROP TABLE IF EXISTS users"
    val statement = connection.createStatement(dropTableSQL)
    statement.execute().awaitFirstOrNull()
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

private suspend fun executeInsert(connection: Connection, name: String, email: String) {
    val sql = "INSERT INTO users (name, email) VALUES (\$1, \$2) RETURNING id"
    val statement = connection.createStatement(sql)
    statement.bind(0, name)
    statement.bind(1, email)
    val result = statement.execute().awaitSingle()
    val id = result.map { row, _ ->
            row.get("id", Long::class.java)
        }.awaitSingle()
    println("Inserted user with id: $id")
}

private suspend fun selectAll(connection: Connection) {
    val sql = "SELECT * FROM users"
    val statement = connection.createStatement(sql)
    val result = statement.execute().awaitSingle()
    result.map { row, _ ->
        val id = row.get("id", Long::class.java)
        val name = row.get("name", String::class.java)
        val email = row.get("email", String::class.java)
        println("User: id=$id, name=$name, email=$email")
    }.awaitLast()
}

fun main() = runBlocking {
    val connectionFactory = createConnectionFactory()
    val connection = connectionFactory.create().awaitSingle()
    deleteUserTable(connection)
    createUserTable(connection)
    executeInsert(connection, "Alice", "alice@example.com")
    executeInsert(connection, "Bob", "bob@example.com")
    selectAll(connection)
}
