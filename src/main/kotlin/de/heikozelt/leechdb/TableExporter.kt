package de.heikozelt.leechdb

import mu.KotlinLogging
import oracle.sql.TIMESTAMP
import java.io.*
import java.math.BigDecimal
import java.sql.Blob
import java.sql.Clob
import java.sql.Connection
import java.sql.Timestamp
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.*
import java.util.concurrent.BlockingQueue
import java.util.zip.CRC32
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream
import kotlin.system.exitProcess

class TableExporter(
    private val konf: Konfiguration,
    private val conn: Connection,
    private val tables: BlockingQueue<String>,
    private val checkStream: PrintStream
) : Thread() {

    private val log = KotlinLogging.logger {}

    override fun run() {
        var tabName = tables.take() // is blocking, if no element is available
        while (tabName != POISON) {
            exportTable(
                conn,
                tabName,
                konf.targetPath,
                checkStream,
                konf.excludedColumnsForTable(tabName),
                konf.zip
            )
            tabName = tables.take() // is blocking, if no element is available
        }
    }

    /**
     * Exports a table to a file.
     * The file contains an INSERT statements for every row.
     */
    private fun exportTable(
        conn: Connection,
        tableName: String,
        targetPath: String,
        checkStream: PrintStream,
        excludedColumns: Set<String>,
        zip: Boolean
    ) {
        log.debug("exportTable(tableName=$tableName) start")
        var blobId = 0
        var clobId = 0
        val stmt = conn.createStatement()
        val sql = "$SELECT_ALL_FROM$tableName"
        log.debug("SQL query: $sql")
        val rs = stmt.executeQuery(sql)
        val meta = rs.metaData
        val numberOfColumns = meta.columnCount
        //log.debug("number of columns: $numberOfColumns")
        val insertSb = StringBuilder()
        insertSb.append("INSERT INTO ${escapeMySqlName(tableName)} (")
        var hasLobColumn = false
        var firstAppended = false
        for (i in 1..numberOfColumns) {
            val colName = meta.getColumnName(i)
            if (colName.lowercase() in excludedColumns) {
                log.debug("  column #$i: $colName: is excluded")
            } else {
                if (firstAppended) {
                    insertSb.append(", ")
                }
                insertSb.append(escapeMySqlName(colName))
                firstAppended = true
                val colType = meta.getColumnTypeName(i)
                if (colType == "BLOB" || colType == "CLOB") {
                    hasLobColumn = true
                    // first build string and then one print() call to be thread safe
                    val checkSb = StringBuilder()
                    checkSb.append("SELECT IF(0 = COUNT(*), 'ok', 'FAILED') AS Result,")
                    checkSb.append(" '${escapeMySqlName(tableName)}.${escapeMySqlName(colName)} LOAD_FILE()' AS Test")
                    checkSb.append(" FROM ${escapeMySqlName(tableName)}")
                    checkSb.append(" WHERE ${escapeMySqlName(colName)} = $IMPORT_ERROR;")
                    checkStream.println(checkSb.toString())
                }
                val colPrecision = meta.getPrecision(i)
                val colScale = meta.getScale(i)
                log.debug("  column #$i: $colName: $colType ($colPrecision $colScale)")
            }
        }
        insertSb.append(") VALUES (")
        val insertPrefix = insertSb.toString()
        val insertSuffix = ");"
        //log.debug("INSERT Statement: $insertPrefix...$insertSuffix")

        var lobsZipStream: ZipOutputStream? = null
        if (hasLobColumn) {
            createDirectory(absoluteLobDirName(targetPath, tableName))
            if (zip) {
                val targetFile = File(absoluteLobZipFileName(targetPath, tableName))
                lobsZipStream = ZipOutputStream(FileOutputStream(targetFile))
            }
        }

        val sqlFile: File
        val sqlStream: OutputStream
        if (zip) {
            sqlFile = File(absoluteInsertSqlZipFileName(targetPath, tableName))
            sqlStream = ZipOutputStream(FileOutputStream(sqlFile))
            sqlStream.putNextEntry(ZipEntry(relativeInsertSqlFileName(tableName)))
        } else {
            sqlFile = File(absoluteInsertSqlFileName(targetPath, tableName))
            sqlStream = FileOutputStream(sqlFile)
        }
        val sqlPrintStream = PrintStream(sqlStream)
        val msg = "INSERT INTO ${tableName.lowercase()}"
        sqlPrintStream.println("SELECT ${mySqlString(msg)} AS '';")

        // Position 0 wird nicht verwendet
        val checkSums = LongArray(numberOfColumns + 1) { 0L }
        val crc32 = CRC32()
        while (rs.next()) {
            val valuesSb = StringBuilder()
            firstAppended = false
            for (i in 1..numberOfColumns) {
                val colName = meta.getColumnName(i)
                if (colName.lowercase() !in excludedColumns) {
                    val obj: Any? = rs.getObject(i)
                    if (firstAppended) {
                        valuesSb.append(", ")
                    }
                    firstAppended = true
                    when (obj) {
                        null -> {
                            valuesSb.append("null")
                        }
                        is BigDecimal -> {
                            val str = obj.toString()
                            valuesSb.append(str)
                            // Example: Oracle: NUMBER(1) --> MySQL: BIT(1)
                            // CRC32(BIT(1)) produces strange results in MySQL
                            if ((meta.getPrecision(i) == 1) && (meta.getScale(i) == 0)) {
                                checkSums[i] += obj.toLong()
                            } else {
                                val ba = str.toByteArray()
                                crc32.reset()
                                crc32.update(ba, 0, ba.size)
                                checkSums[i] = checkSums[i] xor crc32.value
                            }
                        }
                        is Blob -> {
                            //log.debug("is blob")
                            val iStream = obj.binaryStream
                            valuesSb.append(
                                "IFNULL(LOAD_FILE(CONCAT(@import_dir, '${
                                    relativeBlobFileName(
                                        tableName,
                                        blobId
                                    )
                                }')), $IMPORT_ERROR)"
                            )
                            if (zip) {
                                lobsZipStream?.let {
                                    writeInputStreamToZipFile(
                                        iStream,
                                        it,
                                        blobFileName(blobId),
                                        crc32
                                    )
                                }
                            } else {
                                writeInputStreamToFile(
                                    iStream,
                                    absoluteBlobFileName(targetPath, tableName, blobId),
                                    crc32
                                )
                            }
                            blobId++
                            checkSums[i] = checkSums[i] xor crc32.value
                        }
                        is Clob -> {
                            //log.debug("is clob")
                            val reader = obj.characterStream
                            valuesSb.append(
                                "IFNULL(LOAD_FILE(CONCAT(@import_dir, '${
                                    relativeClobFileName(
                                        tableName,
                                        clobId
                                    )
                                }')), $IMPORT_ERROR)"
                            )
                            if (zip) {
                                lobsZipStream?.let {
                                    writeReaderToZipFile(reader, it, clobFileName(clobId), crc32)
                                }
                            } else {
                                writeReaderToFile(reader, absoluteClobFileName(targetPath, tableName, clobId), crc32)
                            }
                            checkSums[i] = checkSums[i] xor crc32.value
                            clobId++
                        }
                        is String -> {
                            appendMySqlString(valuesSb, obj)
                            val ba = obj.toByteArray()
                            crc32.reset()
                            crc32.update(ba, 0, ba.size)
                            checkSums[i] = checkSums[i] xor crc32.value
                        }
                        is Timestamp -> { // todo: Genauigkeit beachten
                            // '2021-12-31 23:59:59.123456789'
                            val dateTime = mySqlTimestampFormat.format(obj.toInstant())
                            valuesSb.append(dateTime)
                        }
                        is TIMESTAMP -> {
                            //log.debug("spezielles Oracle-TIMESTAMP-Format")
                            val dateTime = mySqlTimestampFormat.format(obj.timestampValue().toInstant())
                            valuesSb.append(dateTime)
                        }
                        else -> {
                            log.error("Unknown column type! ${obj::class.qualifiedName}")
                            exitProcess(1)
                        }
                    }
                }
            }
            val values = valuesSb.toString()
            //log.debug("values: $values")
            val insertStatement = "$insertPrefix$values$insertSuffix"
            //log.debug("insertStatement='$insertStatement'")
            sqlPrintStream.println(insertStatement)
        }
        lobsZipStream?.close()
        for (i in 1..numberOfColumns) {
            val colName = meta.getColumnName(i)
            if (colName.lowercase() !in excludedColumns) {
                val colType = meta.getColumnTypeName(i)
                // maybe NUMBER(1) in Oracle --> BIT(1) in MySQL
                if ((colType == "NUMBER") && (meta.getPrecision(i) == 1) && (meta.getScale(i) == 0)) {
                    checkStream.print("SELECT IF(${checkSums[i]} = IFNULL(SUM(${escapeMySqlName(colName)}), 0), 'ok', 'FAILED') As Result,")
                    checkStream.print(" '${escapeMySqlName(tableName)}.${escapeMySqlName(colName)} SUM ${checkSums[i]}' AS Test")
                    checkStream.println(" FROM ${escapeMySqlName(tableName)};")
                } else if (
                    colType == "VARCHAR2" ||
                    colType == "CHAR" ||
                    colType == "NVARCHAR2" ||
                    colType == "NCHAR" ||
                    colType == "BLOB" ||
                    colType == "CLOB" ||
                    colType == "NUMBER"
                ) {
                    checkStream.print("SELECT IF(${checkSums[i]} = BIT_XOR(CRC32(${escapeMySqlName(colName)})), 'ok', 'FAILED') As Result,")
                    checkStream.print(" '${escapeMySqlName(tableName)}.${escapeMySqlName(colName)} checksum' AS Test")
                    checkStream.println(" FROM ${escapeMySqlName(tableName)};")
                }
            }
        }
        if (sqlStream is ZipOutputStream) {
            sqlStream.closeEntry()
        }
        sqlPrintStream.close()
        log.debug("exportTable(tableName=$tableName) finished")
    }

    companion object {
        const val POISON = "pOiSoN"
        private const val IMPORT_ERROR = "'iMpOrTeRrOr'"
        private const val SELECT_ALL_FROM = "SELECT * FROM "
        private val mySqlTimestampFormat = DateTimeFormatter
            .ofPattern("''yyyy-MM-dd HH:mm:ss.nnnnnnnnn''")
            .withLocale(Locale.getDefault())
            .withZone(ZoneId.systemDefault())
    }
}