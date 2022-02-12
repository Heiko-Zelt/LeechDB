package de.heikozelt.oracle2mysql

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.zip.CRC32

/**
 * Idee: Checksumme bilden
 * MySQL:
 * - CRC32(column1) liefert Integer.
 * - BIT_XOR(column1) ist Aggregat-Funktion über Interger64.
 * - column1 ^ column2 liefert XOR über 2 Spalten.
 * Fazit: SELECT BIT_XOR(CRC32(column1) ^ CRC32(column2)); liefert Checksumme über gesamte Tabelle
 */

class Crc32Test {
    @Test
    fun crc32_string_hallo() {
        assertEquals(3111268817, crc32OfString("hallo"))
    }

    @Test
    fun crc32_string_Doerte() {
        assertEquals(2455371663, crc32OfString("Dörte"))
    }

    @Test
    fun crc32_string_Euro() {
        assertEquals(2213726422, crc32OfString("€"))
    }

    @Test
    fun crc32_string_EsZett() {
        assertEquals(3250460839, crc32OfString("ß"))
    }

    @Test
    fun crc32_integer_9999() {
        assertEquals(3596399514, crc32OfString(9999.toString()))
    }

    @Test
    fun crc32_float_9_999() {
        assertEquals(1355099722, crc32OfString(9.999.toString()))
    }

    /**
     * MySQL: microseconds precision (6 digits)
     * SELECT CRC23(timestamp("2021-12-31T23:59:59.012349")) =
     * SELECT CRC32(          "2021-12-31 23:59:59.012349" )
     */
    @Test
    fun crc32_timestamp() {
        assertEquals(3506123114, crc32OfString("2021-12-31 23:59:59.012349"))
    }

    /**
     * in MySQL:
     * SELECT CRC32("hallo");
     */
    private fun crc32OfString(str: String): Long {
        val buffer = str.toByteArray()
        val crc32 = CRC32()
        crc32.update(buffer, 0, buffer.size)
        return crc32.value
    }
}