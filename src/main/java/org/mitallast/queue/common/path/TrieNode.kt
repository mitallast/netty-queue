package org.mitallast.queue.common.path

import javaslang.collection.HashMap
import javaslang.collection.Map
import javaslang.control.Option
import org.mitallast.queue.common.strings.QueryStringDecoder
import org.mitallast.queue.common.strings.Strings
import java.util.*

abstract class TrieNode<T, N : TrieNode<T, N>> {

    companion object {
        fun <T> node(): RootTrieNode<T> = RootTrieNode()
        fun <T> wildcard(token: String): WildcardTrieNode<T> = WildcardTrieNode(token)
        fun <T> token(token: String): TokenTrieNode<T> = TokenTrieNode(token)

        fun isNamedWildcard(key: String): Boolean {
            return key.length > 2 && key[0] == '{' && key[key.length - 1] == '}'
        }
    }

    protected val value: Option<T>
    protected val key: String
    protected val wildcard: Array<WildcardTrieNode<T>>
    protected val children: Array<TokenTrieNode<T>>

    constructor(key: String) {
        this.key = key
        this.value = Option.none()
        this.wildcard = emptyArray()
        this.children = emptyArray()
    }

    constructor(key: String, value: Option<T>, wildcard: Array<WildcardTrieNode<T>>, children: Array<TokenTrieNode<T>>) {
        this.key = key
        this.value = value
        this.wildcard = wildcard
        this.children = children
    }

    abstract fun isToken(token: String): Boolean
    abstract fun withValue(value: T): N
    abstract fun withWildcard(wildcard: Array<WildcardTrieNode<T>>): N
    abstract fun withChildren(children: Array<TokenTrieNode<T>>): N

    fun insert(path: String, value: T): N {
        val parts = Strings.splitStringToArray(path, '/')
        if (parts.isEmpty()) {
            return withValue(value)
        }
        return insert(parts, 0, value)
    }

    protected fun insert(path: Array<String>, index: Int, value: T): N {
        if (index > path.size) {
            throw ArrayIndexOutOfBoundsException(index)
        } else if (index == path.size) {
            return withValue(value)
        } else {
            val token = path[index]
            if (TrieNode.isNamedWildcard(token)) {
                for ((i, child) in wildcard.withIndex()) {
                    if (child.isToken(token)) {
                        val w = Arrays.copyOf(wildcard, wildcard.size)
                        w[i] = child.insert(path, index + 1, value)
                        return withWildcard(w)
                    }
                }
                val w = Arrays.copyOf(wildcard, wildcard.size + 1)
                w[wildcard.size] = TrieNode.wildcard<T>(token).insert(path, index + 1, value)
                return withWildcard(w)
            } else {
                for ((i, child) in children.withIndex()) {
                    if (child.isToken(token)) {
                        val c = Arrays.copyOf(children, children.size)
                        c[i] = child.insert(path, index + 1, value)
                        return withChildren(c)
                    }
                }
                val c = Arrays.copyOf(children, children.size + 1)
                c[children.size] = TrieNode.token<T>(token).insert(path, index + 1, value)
                return withChildren(c)
            }
        }
    }

    fun retrieve(path: String): Pair<Option<T>, Map<String, String>> {
        if (path.isEmpty() || path.length == 1 && path[0] == '/') {
            return Pair(value, HashMap.empty())
        }
        var index = 0
        if (path[0] == '/') {
            index = 1
        }
        return retrieve(path, index)
    }

    protected fun retrieve(path: String, start: Int): Pair<Option<T>, Map<String, String>> {
        var len = path.length
        if (start >= len) {
            return Pair(Option.none(), HashMap.empty())
        }
        var end = path.indexOf('/', start)
        val isEnd = end == -1
        if (end == len - 1) { // ends with
            len -= 1
            end = len
        } else if (isEnd) {
            end = len
        }
        for (child in children) {
            if (isEnd && child.value.isEmpty) {
                continue
            }
            if (!isEnd && child.children.isEmpty() && child.wildcard.isEmpty()) {
                continue
            }
            if (child.keyEquals(path, start, end)) {
                return if (isEnd) {
                    Pair(child.value, HashMap.empty())
                } else {
                    val res = child.retrieve(path, end + 1)
                    if (res.first.isDefined) {
                        res
                    } else {
                        break
                    }
                }
            }
        }
        for (child in wildcard) {
            if (isEnd && child.value.isDefined) {
                val params = child.params(path, start, end)
                return Pair(child.value, params)
            }
            val (res, p) = child.retrieve(path, end + 1)
            if (res.isDefined) {
                val params = child.params(path, start, end).merge(p)
                return Pair(res, params)
            }
        }
        return Pair(Option.none(), HashMap.empty())
    }

    fun prettyPrint() = prettyPrint(0, "", true)

    protected fun prettyPrint(level: Int, prefix: String, last: Boolean) {
        print(prefix)
        if (level > 0) {
            if (last) {
                print("└── ")
            } else {
                print("├── ")
            }
        }
        println("$key [$value]")
        val lastNode: TrieNode<*, *>? = when {
            wildcard.isNotEmpty() -> wildcard.last()
            children.isNotEmpty() -> children.last()
            else -> null
        }
        val childPrefix =
            if (level > 0) prefix + if (last) "    " else "├── "
            else prefix

        for (child in children) {
            child.prettyPrint(level + 1, childPrefix, lastNode === child)
        }
        for (child in wildcard) {
            child.prettyPrint(level + 1, childPrefix, lastNode === child)
        }
        if (level == 0) {
            println()
        }
    }
}

class WildcardTrieNode<T> : TrieNode<T, WildcardTrieNode<T>> {
    private val token: String
    private val tokenWildcard: String

    constructor(token: String) : super(token) {
        this.token = token
        this.tokenWildcard = token.substring(1, token.length - 1)
    }

    constructor(
        token: String,
        value: Option<T>,
        wildcard: Array<WildcardTrieNode<T>>,
        children: Array<TokenTrieNode<T>>) : super(token, value, wildcard, children) {
        this.token = token
        this.tokenWildcard = token.substring(1, token.length - 1)
    }

    override fun isToken(token: String): Boolean {
        return this.token == token
    }

    override fun withValue(value: T): WildcardTrieNode<T> {
        return WildcardTrieNode(token, Option.some(value), wildcard, children)
    }

    override fun withWildcard(wildcard: Array<WildcardTrieNode<T>>): WildcardTrieNode<T> {
        return WildcardTrieNode(token, value, wildcard, children)
    }

    override fun withChildren(children: Array<TokenTrieNode<T>>): WildcardTrieNode<T> {
        return WildcardTrieNode(token, value, wildcard, children)
    }

    fun params(value: String, start: Int, end: Int): Map<String, String> {
        val decoded =  QueryStringDecoder.decodeComponent(value.subSequence(start, end))
        return HashMap.of(tokenWildcard, decoded)
    }
}

class TokenTrieNode<T> : TrieNode<T, TokenTrieNode<T>> {
    private val token: String

    constructor(token: String) : super(token) {
        this.token = token
    }

    constructor(
        token: String,
        value: Option<T>,
        wildcard: Array<WildcardTrieNode<T>>,
        children: Array<TokenTrieNode<T>>) : super(token, value, wildcard, children) {
        this.token = token
    }

    override fun isToken(token: String): Boolean {
        return this.token == token
    }

    override fun withValue(value: T): TokenTrieNode<T> {
        return TokenTrieNode(token, Option.some(value), wildcard, children)
    }

    override fun withWildcard(wildcard: Array<WildcardTrieNode<T>>): TokenTrieNode<T> {
        return TokenTrieNode(token, value, wildcard, children)
    }

    override fun withChildren(children: Array<TokenTrieNode<T>>): TokenTrieNode<T> {
        return TokenTrieNode(token, value, wildcard, children)
    }

    fun keyEquals(sequence: String, start: Int, end: Int): Boolean {
        return end - start == token.length && token.regionMatches(0, sequence, start, token.length)
    }
}

class RootTrieNode<T> : TrieNode<T, RootTrieNode<T>> {
    constructor() : super("/")

    constructor(
        value: Option<T>,
        wildcard: Array<WildcardTrieNode<T>>,
        children: Array<TokenTrieNode<T>>) : super("/", value, wildcard, children)

    override fun isToken(token: String): Boolean {
        return "/" == token
    }

    override fun withValue(value: T): RootTrieNode<T> {
        return RootTrieNode(Option.some(value), wildcard, children)
    }

    override fun withWildcard(wildcard: Array<WildcardTrieNode<T>>): RootTrieNode<T> {
        return RootTrieNode(value, wildcard, children)
    }

    override fun withChildren(children: Array<TokenTrieNode<T>>): RootTrieNode<T> {
        return RootTrieNode(value, wildcard, children)
    }
}