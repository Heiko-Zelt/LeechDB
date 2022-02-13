package de.heikozelt.oracle2mysql

import mu.KotlinLogging
import oracle.jdbc.datasource.impl.OracleDataSource
import oracle.sql.TIMESTAMP
import java.io.*
import java.math.BigDecimal
import java.sql.Blob
import java.sql.Clob
import java.sql.Connection
import java.sql.Timestamp
import java.text.DecimalFormat
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.*
import java.util.zip.CRC32
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream
import kotlin.system.exitProcess

private const val SELECT_TABLE_NAMES = "SELECT TABLE_NAME FROM USER_TABLES ORDER BY TABLE_NAME"
private const val SELECT_ALL_FROM = "SELECT * FROM "
private const val SELECT_COUNT_FROM = "SELECT COUNT(*) FROM "
private const val TRUNCATE_TABLE = "TRUNCATE TABLE "

/**
 * This MySQL-value is inserted, if LOAD_FILE() returns NULL = file not found
 */
private const val IMPORT_ERROR = "'iMpOrTeRrOr'"

private val log = KotlinLogging.logger {}
private val byteBuffer = ByteArray(1 shl 13) { 0 }
private val charBuffer = CharArray(1 shl 12) { '?' }
private val mySqlTimestampFormat = DateTimeFormatter
    .ofPattern("''yyyy-MM-dd HH:mm:ss.nnnnnnnnn''")
    .withLocale(Locale.getDefault())
    .withZone(ZoneId.systemDefault())


/**
 * This little program helps to migrate data from an Oracle to a MySQL/Maria database.
 * It reads a schema from an Oracle database via JDBC and writes the contents of the tables into files.
 * The files contain MySQL-INSERT-Statements to be imported via mysql command line client.
 * If columns contains large objects (CLOBs or BLOBs) they are written to separate files and referenced.
 * Reads configuration file "export.properties" from current working directory.
 * It is assumed that the schema in the target database already exists e.g. created using "Liquibase".
 * Before importing the new data foreign key constraints must be switched of temporarily.
 */
fun main() {
    val startTime = System.nanoTime()
    log.debug("Starting...")

    val konf = Konfiguration().apply {
        loadFromPropertiesFile()
        assertEmptyDirectory(targetPath)
    }

    val ds = OracleDataSource()
    ds.url = konf.url
    val conn = ds.getConnection(konf.user, konf.password)
    val stmt = conn.createStatement()
    log.debug("SQL query: $SELECT_TABLE_NAMES")
    val tablesRs = stmt.executeQuery(SELECT_TABLE_NAMES)
    val truncatesFile = FileWriter(absoluteTruncatesSqlFileName(konf.targetPath))
    truncatesFile.write("SELECT 'TRUNCATE TABLES' AS '';\n")
    val mainFile = FileWriter(absoluteMainSqlFileName(konf.targetPath))
    val checkFile = FileWriter(absoluteCheckSqlFileName(konf.targetPath))
    checkFile.write("SELECT 'VALIDITY CHECKS' AS '';\n")
    mainFile.write(mainHead(konf.url, konf.user))
    mainFile.write("source ${relativeTruncatesSqlFileName()}\n")
    while (tablesRs.next()) {
        val tableName = tablesRs.getString("TABLE_NAME")
        log.debug("tableName: $tableName")
        if (tableName.startsWith("SYS_IOT_OVER_")) {
            log.debug("  is overflow table")
        } else if (tableName.lowercase() in konf.excludeTables) {
            log.debug("  is excluded table")
        } else {
            truncatesFile.write("$TRUNCATE_TABLE${escapeMySqlName(tableName)};\n")
            val rowCount = countRows(conn, tableName)
            checkFile.write("SELECT IF($rowCount = COUNT(*), 'ok', 'FAILED') AS Result,")
            checkFile.write(" '${escapeMySqlName(tableName)} COUNT $rowCount' AS Test")
            checkFile.write(" FROM ${escapeMySqlName(tableName)};\n")
            if (rowCount == 0) {
                log.debug("  is empty table")
            } else {
                mainFile.write("source ${relativeInsertSqlFileName(tableName)}\n")
                exportTable(conn, tableName, konf.targetPath, checkFile, konf.excludedColumnsForTable(tableName))
            }
        }
    }
    mainFile.write(mainFoot())
    mainFile.close()
    checkFile.close()
    truncatesFile.close()
    log.info("Export finished successfully. :-)")
    val endTime = System.nanoTime()
    val elapsed = (endTime.toFloat() - startTime.toFloat()) / 1_000_000_000
    val deci = DecimalFormat("#,###.00")
    log.info("Elapsed time: ${deci.format(elapsed)} sec")
}

/**
 * counts rows of table
 */
private fun countRows(conn: Connection, tableName: String): Int {
    //log.debug("countRows(tableName=$tableName)")
    val stmt = conn.createStatement()
    val sql = "$SELECT_COUNT_FROM$tableName"
    log.debug("SQL query: $sql")
    val rs = stmt.executeQuery(sql)
    rs.next()
    val rowCount = rs.getInt(1)
    rs.close()
    return rowCount
}

/**
 * Exports a table to a file.
 * The file contains an INSERT statements for every row.
 */
private fun exportTable(
    conn: Connection,
    tableName: String,
    targetPath: String,
    checkFile: FileWriter,
    excludedColumns: Set<String>
) {
    log.debug("exportTable(tableName=$tableName)")
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
                checkFile.write("SELECT IF(0 = COUNT(*), 'ok', 'FAILED') AS Result,")
                checkFile.write(" '${escapeMySqlName(tableName)}.${escapeMySqlName(colName)} LOAD_FILE()' AS Test")
                checkFile.write(" FROM ${escapeMySqlName(tableName)}")
                checkFile.write(" WHERE ${escapeMySqlName(colName)} = $IMPORT_ERROR;\n")
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

    if (hasLobColumn) {
        createDirectory(absoluteLobDirName(targetPath, tableName))
    }

    val sqlFile = File(absoluteInsertSqlFileName(targetPath, tableName))
    val sqlStream = PrintStream(sqlFile)
    val msg = "INSERT INTO ${tableName.lowercase()}"
    sqlStream.println("SELECT ${mySqlString(msg)} AS '';")

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
                        writeInputStreamToZipFile(
                            iStream,
                            absoluteBlobFileNameZipped(targetPath, tableName, blobId),
                            blobFileName(blobId),
                            crc32
                        )
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
                        writeReaderToFile(reader, absoluteClobFileName(targetPath, tableName, clobId), crc32)
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
        sqlStream.println(insertStatement)
    }
    for (i in 1..numberOfColumns) {
        val colName = meta.getColumnName(i)
        if (colName.lowercase() !in excludedColumns) {
            val colType = meta.getColumnTypeName(i)
            // maybe NUMBER(1) in Oracle --> BIT(1) in MySQL
            if ((colType == "NUMBER") && (meta.getPrecision(i) == 1) && (meta.getScale(i) == 0)) {
                checkFile.write("SELECT IF(${checkSums[i]} = IFNULL(SUM(${escapeMySqlName(colName)}), 0), 'ok', 'FAILED') As Result,")
                checkFile.write(" '${escapeMySqlName(tableName)}.${escapeMySqlName(colName)} SUM' AS Test")
                checkFile.write(" FROM ${escapeMySqlName(tableName)};\n")
            } else if (
                colType == "VARCHAR2" ||
                colType == "CHAR" ||
                colType == "NVARCHAR2" ||
                colType == "NCHAR" ||
                colType == "BLOB" ||
                colType == "CLOB" ||
                colType == "NUMBER"
            ) {
                checkFile.write("SELECT IF(${checkSums[i]} = BIT_XOR(CRC32(${escapeMySqlName(colName)})), 'ok', 'FAILED') As Result,")
                checkFile.write(" '${escapeMySqlName(tableName)}.${escapeMySqlName(colName)} checksum' AS Test")
                checkFile.write(" FROM ${escapeMySqlName(tableName)};\n")
            }
        }
    }
    sqlStream.close()
}

/*
 * for BLOBs
fun writeInputStreamToFile(iStream: InputStream, filePath: String) {
    //log.debug("de.heikozelt.oracle2mysql.writeInputStreamToFile(fileName='$filePath')")
    val targetFile = File(filePath)
    val oStream = FileOutputStream(targetFile)
    var bytesRead: Int
    var chunks = 0
    while (iStream.read(byteBuffer).also { bytesRead = it } != -1) {
        chunks++
        oStream.write(byteBuffer, 0, bytesRead)
    }
    //log.debug("chunks: $chunks")
    iStream.close()
    oStream.close()
}
*/

/**
 * for BLOBs
 */
fun writeInputStreamToZipFile(iStream: InputStream, zipFilePath: String, fileName: String, crc32: CRC32) {
    //log.debug("writeInputStreamToZipFile(fileName='$filePath')")
    val targetFile = File(zipFilePath)
    val zipoStream = ZipOutputStream(FileOutputStream(targetFile))
    zipoStream.putNextEntry(ZipEntry(fileName))
    var bytesRead: Int
    //var chunks = 0
    crc32.reset()
    while (iStream.read(byteBuffer).also { bytesRead = it } != -1) {
        //chunks++
        crc32.update(byteBuffer, 0, bytesRead)
        zipoStream.write(byteBuffer, 0, bytesRead)
    }
    //log.debug("chunks: $chunks")
    iStream.close()
    zipoStream.closeEntry()
    zipoStream.close()
}

/**
 * for CLOBs
 */
fun writeReaderToFile(reader: Reader, filePath: String, crc32: CRC32) {
    log.debug("writeInputStreamToFile(fileName='$filePath')")
    val targetFile = File(filePath)
    val writer = FileWriter(targetFile)
    var charsRead: Int
    //var chunks = 0
    crc32.reset()
    while (reader.read(charBuffer).also { charsRead = it } != -1) {
        //chunks++
        val ba = if (charsRead == charBuffer.size) {
            String(charBuffer).toByteArray()
        } else {
            String(charBuffer.sliceArray(0 until charsRead)).toByteArray()
        }
        crc32.update(ba, 0, ba.size)
        writer.write(charBuffer, 0, charsRead)
    }
    //log.debug("chunks: $chunks")
    //log.debug("crc32.value=${crc32.value}")
    reader.close()
    writer.close()
}

/**
 * checks if target is an empty directory in the file system.
 * We don't want to overwrite existing data.
 */
fun assertEmptyDirectory(dirPath: String) {
    log.debug("assertEmptyDirectory(dirName='$dirPath')")
    val dir = File(dirPath)
    if (!dir.exists()) {
        throw IllegalArgumentException("Target path doesn't exist!")
    }
    if (!dir.isDirectory) {
        throw IllegalArgumentException("Target path is not a directory!")
    }
    val dirEntries: Array<out String> = dir.list() ?: throw IllegalArgumentException("Error reading target dir!")
    if (dirEntries.isNotEmpty()) {
        throw IllegalArgumentException("The target directory is not empty!")
    }
}

/**
 * creates a directory in the file system
 */
fun createDirectory(dirPath: String) {
    val dir = File(dirPath)
    val created = dir.mkdir()
    if (!created) {
        throw IOException("Couldn't create directory!")
    }
}

/**
 * there are crazy table and column names with special characters.
 * they have to be escaped.
 * by the way, convert to lower case for better readablity.
 */
fun escapeMySqlName(name: String): String {
    val lower = name.lowercase()
    return if (lower.contains('#')) {
        "`$lower`"
    } else {
        lower
    }
}

/**
 * The name of file, which should be used with the mysql client.
 * It includes all other SQL files.
 */
fun relativeMainSqlFileName(): String {
    return "main.sql"
}

fun relativeCheckSqlFileName(): String {
    return "checks.sql"
}

/**
 * Name of the file with the TRUNCATE statements to empty target database schema.
 */
fun relativeTruncatesSqlFileName(): String {
    return "truncate_all.sql"
}

/**
 * Name of the file with the INSERT statements for a table.
 */
fun relativeInsertSqlFileName(tableName: String): String {
    return "insert_into_${tableName.lowercase()}.sql"
}

/**
 * Name of a BLOB file.
 */
fun blobFileName(blobId: Int): String {
    return "$blobId.blob"
}

/**
 * Relative path and file name of a BLOB file.
 * The Path is relative to the main SQL file.
 */
fun relativeBlobFileName(tableName: String, blobId: Int): String {
    return "lobs_${tableName.lowercase()}${File.separator}${blobFileName(blobId)}"
}

/**
 * Relative path and file name of a BLOB file.
 * The Path is relative to the main SQL file.
 */
fun relativeBlobFileNameZipped(tableName: String, blobId: Int): String {
    return "${relativeBlobFileName(tableName, blobId)}.zip"
}

/**
 * Relative path and file name of a CLOB file.
 * The Path is relative to the main SQL file.
 */
fun relativeClobFileName(tableName: String, clobId: Int): String {
    return "lobs_${tableName.lowercase()}${File.separator}$clobId.clob"
}

fun absoluteMainSqlFileName(targetPath: String): String {
    return "$targetPath${File.separator}${relativeMainSqlFileName()}"
}

fun absoluteTruncatesSqlFileName(targetPath: String): String {
    return "$targetPath${File.separator}${relativeTruncatesSqlFileName()}"
}

fun absoluteCheckSqlFileName(targetPath: String): String {
    return "$targetPath${File.separator}${relativeCheckSqlFileName()}"
}

fun absoluteInsertSqlFileName(targetPath: String, tableName: String): String {
    return "$targetPath${File.separator}${relativeInsertSqlFileName(tableName)}"
}

fun absoluteLobDirName(targetPath: String, tableName: String): String {
    return "$targetPath${File.separator}lobs_${tableName.lowercase()}"
}

fun absoluteBlobFileNameZipped(targetPath: String, tableName: String, blobId: Int): String {
    return "$targetPath${File.separator}${relativeBlobFileNameZipped(tableName, blobId)}"
}

fun absoluteClobFileName(targetPath: String, tableName: String, clobId: Int): String {
    return "$targetPath${File.separator}${relativeClobFileName(tableName, clobId)}"
}

/**
 * Head of the main SQL file with comments and initial SQL statements.
 */
fun mainHead(jdbcUrl: String, schema: String): String {
    val dateTime = LocalDateTime.now()
    val dateTimeStr = dateTime.format(DateTimeFormatter.ofPattern("y-MM-dd H:m:ss"))
    val txt = StringBuilder()
    txt.append("-- this dump file set was generated using Oracle2MySql, author: Heiko Zelt\n")
    txt.append("-- source database: $jdbcUrl\n")
    txt.append("-- schema: $schema\n")
    txt.append("-- exported: ${dateTimeStr}\n")
    txt.append('\n')
    txt.append("SET @import_dir='/tmp/import/';\n")
    txt.append('\n')
    txt.append("SET foreign_key_checks=0;\n")
    txt.append("SET autocommit=1;\n")
    txt.append('\n')
    return txt.toString()
}

/**
 * Foot of the main SQL file with comments and initial SQL statements.
 */
fun mainFoot(): String {
    val txt = StringBuilder()
    txt.append('\n')
    txt.append("SET foreign_key_checks=1;\n")
    txt.append("source ${relativeCheckSqlFileName()}\n")
    return txt.toString()
}

/**
 * returns a string literal in MySQL format
 */
fun mySqlString(str: String): String {
    val sb = StringBuilder()
    appendMySqlString(sb, str)
    return sb.toString()
}

/**
 * Appends a String in MySQL-Format to the StringBuilder.
 */
fun appendMySqlString(sb: StringBuilder, str: String) {
    sb.append('\'') // beginning of string
    for (c in str) {
        when (c) {
            '\'' -> sb.append("""\'""") // apostroph
            '\\' -> sb.append("""\\""") // backslash
            '\n' -> sb.append("""\n""") // new line
            '\r' -> sb.append("""\r""") // carriage return
            '\t' -> sb.append("""\t""") // tab
            // are there other characters which have to be escaped?
            else -> sb.append(c)
        }
    }
    sb.append('\'') // end of string
}