package org.mitallast.queue.common.strings

import java.util.*

object Strings {

    private val EMPTY_ARRAY = arrayOf<String>()

    fun splitStringToArray(s: String, delimiter: Char): Array<String> {
        if (s.isEmpty()) {
            return Strings.EMPTY_ARRAY
        }
        var match0: String? = null
        var match1: String? = null
        var match2: String? = null
        var match3: String? = null
        var match4: String? = null
        var match5: String? = null
        var match6: String? = null
        var match7: String? = null

        var matches: ArrayList<String>? = null
        var matched = 0

        val length = s.length
        var start = 0
        for (index in 0 until length) {
            val currentChar = s[index]
            if (currentChar == delimiter) {
                if (start < index) {
                    val match = s.substring(start, index)
                    when(matched) {
                        0 -> match0 = match
                        1 -> match1 = match
                        2 -> match2 = match
                        3 -> match3 = match
                        4 -> match4 = match
                        5 -> match5 = match
                        6 -> match6 = match
                        7 -> match7 = match
                        8 -> {
                            matches = ArrayList(8)
                            matches.add(match0!!)
                            matches.add(match1!!)
                            matches.add(match2!!)
                            matches.add(match3!!)
                            matches.add(match4!!)
                            matches.add(match5!!)
                            matches.add(match6!!)
                            matches.add(match7!!)
                            matches.add(match)
                        }
                        else -> matches!!.add(match)
                    }
                    matched++
                }
                start = index + 1
            }
        }
        if (start < length) {
            val match = s.substring(start, length)
            when(matched) {
                0 -> match0 = match
                1 -> match1 = match
                2 -> match2 = match
                3 -> match3 = match
                4 -> match4 = match
                5 -> match5 = match
                6 -> match6 = match
                7 -> match7 = match
                8 -> {
                    matches = ArrayList(8)
                    matches.add(match0!!)
                    matches.add(match1!!)
                    matches.add(match2!!)
                    matches.add(match3!!)
                    matches.add(match4!!)
                    matches.add(match5!!)
                    matches.add(match6!!)
                    matches.add(match7!!)
                    matches.add(match)
                }
                else -> matches!!.add(match)
            }
            matched++
        }
        return when(matched) {
            0 -> EMPTY_ARRAY
            1 -> arrayOf(match0!!)
            2 -> arrayOf(match0!!,match1!!)
            3 -> arrayOf(match0!!,match1!!,match2!!)
            4 -> arrayOf(match0!!,match1!!,match2!!,match3!!)
            5 -> arrayOf(match0!!,match1!!,match2!!,match3!!,match4!!)
            6 -> arrayOf(match0!!,match1!!,match2!!,match3!!,match4!!,match5!!)
            7 -> arrayOf(match0!!,match1!!,match2!!,match3!!,match4!!,match5!!,match6!!)
            8 -> arrayOf(match0!!,match1!!,match2!!,match3!!,match4!!,match5!!,match6!!,match7!!)
            else -> matches!!.toTypedArray()
        }
    }
}
