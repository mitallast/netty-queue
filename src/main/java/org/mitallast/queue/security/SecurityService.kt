package org.mitallast.queue.security

import com.google.inject.Inject
import com.typesafe.config.Config
import org.mitallast.queue.common.Hex.parseHexBinary
import java.security.*
import java.security.spec.PKCS8EncodedKeySpec
import java.security.spec.X509EncodedKeySpec
import javax.crypto.Cipher

class SecurityService @Inject constructor(config: Config) {

    private val publicKey: PublicKey
    private val privateKey: PrivateKey

    init {
        val publicKeyStr = config.getString("security.rsa.public")
        val privateKeyStr = config.getString("security.rsa.private")
        val publicKeyBytes = parseHexBinary(publicKeyStr)
        val privateKeyBytes = parseHexBinary(privateKeyStr)

        val kf = KeyFactory.getInstance("RSA")
        val publicKeySpec = X509EncodedKeySpec(publicKeyBytes)
        val privateKeySpec = PKCS8EncodedKeySpec(privateKeyBytes)
        publicKey = kf.generatePublic(publicKeySpec)
        privateKey = kf.generatePrivate(privateKeySpec)
    }

    fun sign(data: ByteArray): ByteArray {
        val signSignature = Signature.getInstance("SHA1withRSA")
        signSignature.initSign(privateKey, SecureRandom())
        signSignature.update(data)
        return signSignature.sign()
    }

    fun verify(data: ByteArray, sign: ByteArray): Boolean {
        val signSignature = Signature.getInstance("SHA1withRSA")
        signSignature.initVerify(publicKey)
        signSignature.update(data)
        return signSignature.verify(sign)
    }

    fun encrypt(data: ByteArray): ByteArray {
        val cipher = Cipher.getInstance("RSA/ECB/PKCS1Padding")
        cipher.init(Cipher.ENCRYPT_MODE, publicKey, SecureRandom())
        return cipher.doFinal(data)
    }

    fun decrypt(encrypted: ByteArray): ByteArray {
        val cipher = Cipher.getInstance("RSA/ECB/PKCS1Padding")
        cipher.init(Cipher.DECRYPT_MODE, privateKey)
        return cipher.doFinal(encrypted)
    }

    fun ecdh(): ECDHFlow {
        return ECDHFlow(this)
    }
}
