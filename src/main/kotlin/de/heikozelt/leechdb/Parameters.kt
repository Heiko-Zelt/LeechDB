package de.heikozelt.leechdb

object Parameters {
    const val PREFIX = "leechdb."

    // Keys
    const val SOURCE_URL =  PREFIX + "source.url"
    const val SOURCE_USER = PREFIX + "source.user"
    const val SOURCE_PASSWORD = PREFIX + "source.password"
    const val EXCLUDE_TABLES = PREFIX + "exclude.tables"
    const val EXCLUDE_COLUMNS = PREFIX + "exclude.columns"
    const val TARGET_PATH = PREFIX + "target.path"
    const val TARGET_ZIP = PREFIX + "target.zip"

    val allowed = arrayOf(
        SOURCE_URL, SOURCE_USER, SOURCE_PASSWORD, EXCLUDE_TABLES, EXCLUDE_COLUMNS, TARGET_PATH, TARGET_ZIP
    )

    // Values
    const val NO = "no"
    const val YES = "yes"
}