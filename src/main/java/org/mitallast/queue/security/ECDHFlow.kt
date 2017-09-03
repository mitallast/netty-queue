package org.mitallast.queue.security

import io.netty.util.AttributeKey
import org.conscrypt.OpenSSLProvider
import java.lang.reflect.Field
import java.lang.reflect.Modifier
import java.nio.ByteBuffer
import java.security.*
import java.security.spec.ECGenParameterSpec
import java.security.spec.X509EncodedKeySpec
import java.util.*
import java.util.concurrent.CompletableFuture
import javax.crypto.Cipher
import javax.crypto.KeyAgreement
import javax.crypto.SecretKey
import javax.crypto.spec.IvParameterSpec
import javax.crypto.spec.SecretKeySpec

class ECDHFlow constructor(private val securityService: SecurityService) {

    private enum class State {
        START, AGREEMENT
    }

    private val keyPair: KeyPair
    private var state: State? = null
    private var otherPublicKey: PublicKey? = null
    private var secretKey: SecretKey? = null
    private val signSignature: Signature
    private val verifySignature: Signature
    private val encryptCipher: Cipher
    private val decryptCipher: Cipher
    private val agreementFuture: CompletableFuture<Void>

    init {
        this.signSignature = Signature.getInstance(ECC_SIGNATURE, PROVIDER)
        this.verifySignature = Signature.getInstance(ECC_SIGNATURE, PROVIDER)
        this.encryptCipher = Cipher.getInstance(AES256)
        this.decryptCipher = Cipher.getInstance(AES256)
        this.agreementFuture = CompletableFuture()

        val generator = KeyPairGenerator.getInstance(ECC_KEY_TYPE, PROVIDER)
        val spec = ECGenParameterSpec(ECC_CURVE)
        generator.initialize(spec)
        keyPair = generator.genKeyPair()

        state = State.START
    }

    fun requestStart(): ECDHRequest {
        val publicKey = keyPair.public.encoded

        val sign = securityService.sign(publicKey)
        val encrypted = securityService.encrypt(publicKey)
        return ECDHRequest(sign, encrypted)
    }

    fun responseStart(): ECDHResponse {
        val publicKey = keyPair.public.encoded

        val sign = securityService.sign(publicKey)
        val encrypted = securityService.encrypt(publicKey)
        return ECDHResponse(sign, encrypted)
    }

    fun keyAgreement(start: ECDHRequest) {
        val decrypted = securityService.decrypt(start.encodedKey)
        if (!securityService.verify(decrypted, start.sign)) {
            throw IllegalArgumentException("not verified")
        }
        keyAgreement(decrypted)
    }

    fun keyAgreement(start: ECDHResponse) {
        val decrypted = securityService.decrypt(start.encodedKey)
        if (!securityService.verify(decrypted, start.sign)) {
            throw IllegalArgumentException("not verified")
        }
        keyAgreement(decrypted)
    }

    fun keyAgreement(publicKey: ByteArray) {
        if (state != State.START) {
            throw IllegalStateException("dh not in started state")
        }
        this.state = State.AGREEMENT
        val kf = KeyFactory.getInstance(ECC_KEY_TYPE, PROVIDER)
        val pkSpec = X509EncodedKeySpec(publicKey)
        otherPublicKey = kf.generatePublic(pkSpec)

        // Perform key agreement
        val ka = KeyAgreement.getInstance(ECDH_AGREEMENT)
        ka.init(keyPair.private)
        ka.doPhase(otherPublicKey, true)
        val sharedSecret = ka.generateSecret()

        // Derive a key from the shared secret and both public keys
        val hash = MessageDigest.getInstance(DIGEST)
        hash.update(sharedSecret)
        // Simple deterministic ordering
        val keys = Arrays.asList(
            ByteBuffer.wrap(keyPair.public.encoded),
            ByteBuffer.wrap(publicKey))
        Collections.sort(keys)
        hash.update(keys[0])
        hash.update(keys[1])

        val derivedKey = hash.digest()
        secretKey = SecretKeySpec(derivedKey, "AES")

        agreementFuture.complete(null)
    }

    fun sign(data: ByteArray): ByteArray {
        signSignature.initSign(keyPair.private)
        signSignature.update(data)
        return signSignature.sign()
    }

    fun verify(data: ByteArray, sign: ByteArray): Boolean {
        verifySignature.initVerify(otherPublicKey)
        verifySignature.update(data)
        return verifySignature.verify(sign)
    }

    fun encrypt(data: ByteArray): ECDHEncrypted {
        if (state != State.AGREEMENT) {
            throw IllegalStateException("no shared key")
        }

        encryptCipher.init(Cipher.ENCRYPT_MODE, secretKey)
        val params = encryptCipher.parameters
        val iv = params.getParameterSpec(IvParameterSpec::class.java).iv
        val encrypted = encryptCipher.doFinal(data)
        val sign = sign(data)
        return ECDHEncrypted(sign, iv, encrypted)
    }

    fun decrypt(encrypted: ECDHEncrypted): ByteArray {
        if (state != State.AGREEMENT) {
            throw IllegalStateException("no shared key")
        }

        decryptCipher.init(Cipher.DECRYPT_MODE, secretKey, IvParameterSpec(encrypted.iv))
        val decrypted = decryptCipher.doFinal(encrypted.encrypted)
        if (!verify(decrypted, encrypted.sign)) {
            throw IllegalArgumentException("not verified")
        }
        return decrypted
    }

    val isAgreement: Boolean
        get() = state == State.AGREEMENT

    fun agreementFuture(): CompletableFuture<Void> {
        return agreementFuture
    }

    companion object {

        val key = AttributeKey.valueOf<ECDHFlow>("ECDH")

        init {
            try {
                val newMaxKeyLength = Cipher.getMaxAllowedKeyLength("AES")
                if (newMaxKeyLength < 256) {
                    var c = Class.forName("javax.crypto.CryptoAllPermissionCollection")
                    var con = c.getDeclaredConstructor()
                    con.isAccessible = true
                    val allPermissionCollection = con.newInstance()
                    var f = c.getDeclaredField("all_allowed")
                    f.isAccessible = true
                    f.setBoolean(allPermissionCollection, true)

                    c = Class.forName("javax.crypto.CryptoPermissions")

                    con = c.getDeclaredConstructor()
                    con.isAccessible = true
                    val allPermissions = con.newInstance()
                    f = c.getDeclaredField("perms")
                    f.isAccessible = true

                    @Suppress("UNCHECKED_CAST")
                    (f.get(allPermissions) as MutableMap<String, Any>).put("*", allPermissionCollection)

                    c = Class.forName("javax.crypto.JceSecurityManager")
                    f = c.getDeclaredField("defaultPolicy")
                    f.isAccessible = true
                    val mf = Field::class.java.getDeclaredField("modifiers")
                    mf.isAccessible = true
                    mf.setInt(f, f.modifiers and Modifier.FINAL.inv())
                    f.set(null, allPermissions)
                }
            } catch (e: Exception) {
                throw RuntimeException("Failed manually overriding key-length permissions.", e)
            }

            if (Cipher.getMaxAllowedKeyLength("AES") < 256)
                throw RuntimeException("Failed manually overriding key-length permissions.") // hack failed
        }

        private val PROVIDER = "GC"
        private val ECC_KEY_TYPE = "EC"
        private val ECC_CURVE = "secp224r1"
        private val ECC_SIGNATURE = "SHA1withECDSA"
        private val ECDH_AGREEMENT = "ECDH"
        private val AES256 = "AES/CBC/PKCS5Padding"
        private val DIGEST = "SHA-256"

        init {
            Security.addProvider(OpenSSLProvider(PROVIDER))
        }
    }
}
