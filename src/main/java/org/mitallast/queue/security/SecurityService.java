package org.mitallast.queue.security;

import com.google.inject.Inject;
import com.typesafe.config.Config;

import javax.crypto.Cipher;
import java.security.*;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;

import static javax.xml.bind.DatatypeConverter.parseHexBinary;

public class SecurityService {

    private final PublicKey publicKey;
    private final PrivateKey privateKey;

    @Inject
    public SecurityService(Config config) throws GeneralSecurityException {
        String publicKeyStr = config.getString("security.rsa.public");
        String privateKeyStr = config.getString("security.rsa.private");
        byte[] publicKeyBytes = parseHexBinary(publicKeyStr);
        byte[] privateKeyBytes = parseHexBinary(privateKeyStr);

        KeyFactory kf = KeyFactory.getInstance("RSA");
        X509EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(publicKeyBytes);
        PKCS8EncodedKeySpec privateKeySpec = new PKCS8EncodedKeySpec(privateKeyBytes);
        publicKey = kf.generatePublic(publicKeySpec);
        privateKey = kf.generatePrivate(privateKeySpec);
    }

    public byte[] sign(byte[] data) throws GeneralSecurityException {
        Signature signSignature = Signature.getInstance("SHA1withRSA");
        signSignature.initSign(privateKey, new SecureRandom());
        signSignature.update(data);
        return signSignature.sign();
    }

    public boolean verify(byte[] data, byte[] sign) throws GeneralSecurityException {
        Signature signSignature = Signature.getInstance("SHA1withRSA");
        signSignature.initVerify(publicKey);
        signSignature.update(data);
        return signSignature.verify(sign);
    }

    public byte[] encrypt(byte[] data) throws GeneralSecurityException {
        Cipher cipher = Cipher.getInstance("RSA/ECB/PKCS1Padding");
        cipher.init(Cipher.ENCRYPT_MODE, publicKey, new SecureRandom());
        return cipher.doFinal(data);
    }

    public byte[] decrypt(byte[] encrypted) throws GeneralSecurityException {
        Cipher cipher = Cipher.getInstance("RSA/ECB/PKCS1Padding");
        cipher.init(Cipher.DECRYPT_MODE, privateKey);
        return cipher.doFinal(encrypted);
    }

    public ECDHFlow ecdh() throws GeneralSecurityException {
        return new ECDHFlow(this);
    }
}
