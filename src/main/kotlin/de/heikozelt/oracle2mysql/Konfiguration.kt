package de.heikozelt.oracle2mysql

import mu.KotlinLogging
import java.io.FileInputStream
import java.util.*

class Konfiguration {

    private val log = KotlinLogging.logger {}

    var url = ""
    var user = ""
    var password = ""
    var excludeTables = emptySet<String>()
    val excludeColumns = mutableMapOf<String,MutableSet<String>>()
    var targetPath = ""

    fun loadFromPropertiesFile() {
        val fis = FileInputStream("export.properties")
        val props = Properties()
        props.load(fis)
        fromProperties(props)
    }

    fun fromProperties(props: Properties) {

        val urlProp: String? = props.getProperty("oracle2mysql.source.url")
        log.info("url: '$urlProp'")
        if(urlProp == null) {
            throw IllegalArgumentException("missing property oracle2mysql.source.url!")
        }
        url = urlProp

        val userProp: String? = props.getProperty("oracle2mysql.source.user")
        log.info("user: '$userProp'")
        if(userProp == null) {
            throw IllegalArgumentException("missing property oracle2mysql.source.user!")
        }
        user = userProp

        val passwordProp: String = props.getProperty("oracle2mysql.source.password")
            ?: throw IllegalArgumentException("missing property oracle2mysql.source.password!")
        password = passwordProp

        //val schema = props.getProperty("oracle2mysql.source.schema")
        //log.debug("schema: '$schema'")

        val excludeTablesProp: String? = props.getProperty("oracle2mysql.exclude.tables")
        if(excludeTablesProp != null) {
            excludeTables = excludeTablesProp.lowercase().split(",").toSet()
        }
        for(tab in excludeTables) {
            log.info("exclude table: '$tab'")
        }

        val excludeColumnsProp: String? = props.getProperty("oracle2mysql.exclude.columns")
        if(excludeColumnsProp != null) {
            val strings = excludeColumnsProp.lowercase().split(",")
            log.debug("strings: $strings")
            for(str in strings) {
                log.debug("str: $str")
                val tabCol = str.split('.')
                if(tabCol.size != 2) {
                    throw IllegalArgumentException("Number of key/values: ${tabCol.size}, Format: table1.column1")
                }
                val tab = tabCol[0]
                val col = tabCol[1]
                if(excludeColumns.containsKey(tab)) {
                    excludeColumns[tab]?.add(col)
                } else {
                    excludeColumns[tab] = mutableSetOf(col)
                }
            }
        }

        for((tab, cols) in excludeColumns) {
            log.info("exclude columns in table: '$tab'")
            for(col in cols) {
                log.info("exclude column: '$col'")
            }
        }

        val targetPathProp: String? = props.getProperty("oracle2mysql.target.path")
        log.info("targetPath: '${targetPathProp}'")
        if(targetPathProp == null) {
            throw IllegalArgumentException("missing property oracle2mysql.target.path!")
        }
        targetPath = targetPathProp
    }

    fun excludedColumnsForTable(tableName: String): Set<String> {
        val lowerTableName = tableName.lowercase()
        return if(lowerTableName in excludeColumns) {
            excludeColumns[lowerTableName] ?: throw InternalError("null value in map excludeColumns!")
        } else {
            emptySet()
        }
    }
}