package de.heikozelt.oracle2mysql

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.util.*

internal class KonfigurationTest {
    @Test
    fun fromProperties_minimal() {
        val props = Properties().apply {
            setProperty("oracle2mysql.source.url", "jdbc:oracle:thin:@//localhost:1521/orcl")
            setProperty("oracle2mysql.source.user", "scott")
            setProperty("oracle2mysql.source.password", "tiger")
            setProperty("oracle2mysql.target.path", "empty_directory")
        }
        Konfiguration().apply {
            fromProperties(props)
            assertEquals("jdbc:oracle:thin:@//localhost:1521/orcl", url)
            assertEquals("scott", user)
            assertEquals("tiger", password)
            assertEquals(0, excludeTables.size)
            assertEquals(0, excludeColumns.size)
            assertEquals("empty_directory", targetPath)
        }
    }

    @Test
    fun fromProperties_maximal() {
        val props = Properties().apply {
            setProperty("oracle2mysql.source.url", "jdbc:oracle:thin:@//localhost:1521/orcl")
            setProperty("oracle2mysql.source.user", "scott")
            setProperty("oracle2mysql.source.password", "tiger")
            setProperty("oracle2mysql.exclude.tables", "excludeTab1,excludetab2")
            setProperty("oracle2mysql.exclude.columns", "Table1.excol1,table1.exCol2")
            setProperty("oracle2mysql.target.path", """q:\temp\test""")
        }
        Konfiguration().apply {
            fromProperties(props)
            assertEquals("jdbc:oracle:thin:@//localhost:1521/orcl", url)
            assertEquals("scott", user)
            assertEquals("tiger", password)

            assertEquals(2, excludeTables.size)
            assertTrue(excludeTables.contains("excludetab1"))
            assertTrue(excludeTables.contains("excludetab2"))

            assertEquals(1, excludeColumns.size)
            assertNotNull(excludeColumns["table1"])
            excludeColumns["table1"]?.apply {
                assertEquals(2, size)
                assertTrue(contains("excol1"))
                assertTrue(contains("excol2"))
            }

            assertEquals("""q:\temp\test""", targetPath)
        }
    }
}