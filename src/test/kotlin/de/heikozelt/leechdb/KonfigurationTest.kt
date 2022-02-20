package de.heikozelt.leechdb

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.util.*

internal class KonfigurationTest {
    @Test
    fun fromProperties_illegal() {
        val props = Properties().apply {
            setProperty("leechdb.source.url", "jdbc:oracle:thin:@//localhost:1521/orcl")
            setProperty("leechdb.source.user", "scott")
            setProperty("leechdb.source.password", "tiger")
            setProperty("leechdb.target.path", "c:\\temp")
            setProperty("leechdb.illegal.property", "something")
        }
        Konfiguration().apply {
            assertThrows(IllegalArgumentException::class.java) {
                fromProperties(props)
            }
        }
    }

    @Test
    fun fromProperties_minimal() {
        val props = Properties().apply {
            setProperty("leechdb.source.url", "jdbc:oracle:thin:@//localhost:1521/orcl")
            setProperty("leechdb.source.user", "scott")
            setProperty("leechdb.source.password", "tiger")
            setProperty("leechdb.target.path", "empty_directory")
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
            setProperty("leechdb.source.url", "jdbc:oracle:thin:@//localhost:1521/orcl")
            setProperty("leechdb.source.user", "scott")
            setProperty("leechdb.source.password", "tiger")
            setProperty("leechdb.exclude.tables", "excludeTab1,excludetab2")
            setProperty("leechdb.exclude.columns", "Table1.excol1,table1.exCol2")
            setProperty("leechdb.target.path", """q:\temp\test""")
            setProperty("leechdb.target.zip", """no""")
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
            assertFalse(zip)
        }
    }

    @Test
    fun fromProperties_zip_space() {
        val props = Properties().apply {
            setProperty("leechdb.source.url", "jdbc:oracle:thin:@//localhost:1521/orcl")
            setProperty("leechdb.source.user", "scott")
            setProperty("leechdb.source.password", "tiger")
            setProperty("leechdb.exclude.tables", "excludeTab1,excludetab2")
            setProperty("leechdb.exclude.columns", "Table1.excol1,table1.exCol2")
            setProperty("leechdb.target.path", """q:\temp\test""")
            setProperty("leechdb.target.zip", " no ")
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
            assertFalse(zip)
        }
    }
}