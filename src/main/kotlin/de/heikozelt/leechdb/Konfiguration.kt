package de.heikozelt.leechdb

import mu.KotlinLogging
import java.io.FileInputStream
import java.util.*

/**
 * Parsing and validating of configuration
 */
class Konfiguration {

    private val log = KotlinLogging.logger {}

    var url = ""
    var user = ""
    var password = ""
    var excludeTables = emptySet<String>()
    val excludeColumns = mutableMapOf<String, MutableSet<String>>()
    var parallelThreads = 3
    var zip = true
    var targetPath = ""

    fun loadFromPropertiesFile() {
        val fis = FileInputStream(CONFIG_FILENAME)
        val props = Properties()
        props.load(fis)
        fromProperties(props)
    }

    fun fromProperties(props: Properties) {
        for (p in props.propertyNames()) {
            if (p !in Parameters.allowed) {
                throw IllegalArgumentException("Property '$p' is not allowed in Configuration!")
            }
        }

        val urlProp: String? = props.getProperty(Parameters.SOURCE_URL)
        log.info("url: '$urlProp'")
        if (urlProp == null) {
            throw IllegalArgumentException("missing property ${Parameters.SOURCE_URL}!")
        }
        url = urlProp

        val userProp: String? = props.getProperty(Parameters.SOURCE_USER)
        log.info("user: '$userProp'")
        if (userProp == null) {
            throw IllegalArgumentException("missing property ${Parameters.SOURCE_USER}!")
        }
        user = userProp

        val passwordProp: String = props.getProperty(Parameters.SOURCE_PASSWORD)
            ?: throw IllegalArgumentException("missing property ${Parameters.SOURCE_PASSWORD}!")
        password = passwordProp

        //val schema = props.getProperty("oracle2mysql.source.schema")
        //log.debug("schema: '$schema'")

        val excludeTablesProp: String? = props.getProperty(Parameters.EXCLUDE_TABLES)
        if (excludeTablesProp != null) {
            excludeTables = excludeTablesProp.lowercase().split(",").toSet()
        }
        for (tab in excludeTables) {
            log.info("exclude table: '$tab'")
        }

        val excludeColumnsProp: String? = props.getProperty(Parameters.EXCLUDE_COLUMNS)
        if (excludeColumnsProp != null) {
            val strings = excludeColumnsProp.lowercase().split(",")
            log.debug("strings: $strings")
            for (str in strings) {
                log.debug("str: $str")
                val tabCol = str.split('.')
                if (tabCol.size != 2) {
                    throw IllegalArgumentException("Number of key/values: ${tabCol.size}, Format: table1.column1")
                }
                val tab = tabCol[0]
                val col = tabCol[1]
                if (excludeColumns.containsKey(tab)) {
                    excludeColumns[tab]?.add(col)
                } else {
                    excludeColumns[tab] = mutableSetOf(col)
                }
            }
        }

        for ((tab, cols) in excludeColumns) {
            log.info("exclude columns in table: '$tab'")
            for (col in cols) {
                log.info("exclude column: '$col'")
            }
        }

        val parallelThreadsProp: String? = props.getProperty(Parameters.PARALLEL_THREADS)
        log.info("parallelThreads: '${parallelThreadsProp}'")
        if (parallelThreadsProp != null) {
            parallelThreads = parallelThreadsProp.toInt()
            if(parallelThreads <= 0) {
                throw IllegalArgumentException("Property ${Parameters.TARGET_PATH} must be greater than 0!")
            }
        }

        val targetPathProp: String? = props.getProperty(Parameters.TARGET_PATH)
        log.info("targetPath: '${targetPathProp}'")
        if (targetPathProp == null) {
            throw IllegalArgumentException("missing property ${Parameters.TARGET_PATH}!")
        }
        targetPath = targetPathProp

        val targetZipProp: String? = props.getProperty(Parameters.TARGET_ZIP)
        log.info("targetZip: '${targetZipProp}'")
        targetZipProp?.let {
            zip = when (it.trim()) {
                Parameters.NO -> false
                Parameters.YES -> true
                else -> throw IllegalArgumentException(
                    "allowed values for ${Parameters.TARGET_ZIP} are '${Parameters.NO}' or '${Parameters.YES}'"
                )
            }
        }
    }

    fun excludedColumnsForTable(tableName: String): Set<String> {
        val lowerTableName = tableName.lowercase()
        return if (lowerTableName in excludeColumns) {
            excludeColumns[lowerTableName] ?: throw InternalError("null value in map excludeColumns!")
        } else {
            emptySet()
        }
    }

    companion object {
        const val CONFIG_FILENAME = "export.properties"
    }
}