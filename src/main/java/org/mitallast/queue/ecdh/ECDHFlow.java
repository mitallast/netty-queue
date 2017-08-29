package org.mitallast.queue.ecdh;

import javax.crypto.Cipher;
import javax.crypto.KeyAgreement;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.security.*;
import java.security.spec.ECGenParameterSpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@SuppressWarnings("unchecked")
public class ECDHFlow {

    static {
        String errorString = "Failed manually overriding key-length permissions.";
        int newMaxKeyLength;
        try {
            if ((newMaxKeyLength = Cipher.getMaxAllowedKeyLength("AES")) < 256) {
                Class c = Class.forName("javax.crypto.CryptoAllPermissionCollection");
                Constructor con = c.getDeclaredConstructor();
                con.setAccessible(true);
                Object allPermissionCollection = con.newInstance();
                Field f = c.getDeclaredField("all_allowed");
                f.setAccessible(true);
                f.setBoolean(allPermissionCollection, true);

                c = Class.forName("javax.crypto.CryptoPermissions");

                con = c.getDeclaredConstructor();
                con.setAccessible(true);
                Object allPermissions = con.newInstance();
                f = c.getDeclaredField("perms");
                f.setAccessible(true);

                ((Map) f.get(allPermissions)).put("*", allPermissionCollection);

                c = Class.forName("javax.crypto.JceSecurityManager");
                f = c.getDeclaredField("defaultPolicy");
                f.setAccessible(true);
                Field mf = Field.class.getDeclaredField("modifiers");
                mf.setAccessible(true);
                mf.setInt(f, f.getModifiers() & ~Modifier.FINAL);
                f.set(null, allPermissions);

                newMaxKeyLength = Cipher.getMaxAllowedKeyLength("AES");
            }
        } catch (Exception e) {
            throw new RuntimeException(errorString, e);
        }
        if (newMaxKeyLength < 256)
            throw new RuntimeException(errorString); // hack failed
    }

    private enum State {START, AGREEMENT}

    private final KeyPair keyPair;
    private State state;
    private PublicKey otherPublicKey;
    private byte[] derivedKey;

    private final Cipher encryptCipher;
    private final Cipher decryptCipher;
    private final CompletableFuture<Void> agreementFuture;

    public ECDHFlow() throws GeneralSecurityException {
        KeyPairGenerator kpg = KeyPairGenerator.getInstance("EC", "SunEC");
        ECGenParameterSpec ecsp = new ECGenParameterSpec("secp192r1");
        kpg.initialize(ecsp);

        keyPair = kpg.genKeyPair();
        state = State.START;

        encryptCipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        decryptCipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        agreementFuture = new CompletableFuture<>();
    }

    public synchronized byte[] publicKey() throws GeneralSecurityException {
        return keyPair.getPublic().getEncoded();
    }

    public synchronized void keyAgreement(byte[] publicKey) throws GeneralSecurityException {
        if (state != State.START) {
            throw new IllegalStateException("dh not in started state");
        }
        this.state = State.AGREEMENT;
        KeyFactory kf = KeyFactory.getInstance("EC");
        X509EncodedKeySpec pkSpec = new X509EncodedKeySpec(publicKey);
        otherPublicKey = kf.generatePublic(pkSpec);

        // Perform key agreement
        KeyAgreement ka = KeyAgreement.getInstance("ECDH");
        ka.init(keyPair.getPrivate());
        ka.doPhase(otherPublicKey, true);
        byte[] sharedSecret = ka.generateSecret();

        // Derive a key from the shared secret and both public keys
        MessageDigest hash = MessageDigest.getInstance("SHA-256");
        hash.update(sharedSecret);
        // Simple deterministic ordering
        List<ByteBuffer> keys = Arrays.asList(
                ByteBuffer.wrap(keyPair.getPublic().getEncoded()),
                ByteBuffer.wrap(publicKey));
        Collections.sort(keys);
        hash.update(keys.get(0));
        hash.update(keys.get(1));

        derivedKey = hash.digest();
        agreementFuture.complete(null);
    }

    private byte[] sign(byte[] data) throws GeneralSecurityException {
        Signature ecdsa = Signature.getInstance("SHA1withECDSA", "SunEC");
        ecdsa.initSign(keyPair.getPrivate());
        ecdsa.update(data);
        return ecdsa.sign();
    }

    private boolean verify(byte[] data, byte[] sign) throws GeneralSecurityException {
        Signature ecdsa = Signature.getInstance("SHA1withECDSA", "SunEC");
        ecdsa.initVerify(otherPublicKey);
        ecdsa.update(data);
        return ecdsa.verify(sign);
    }

    public Encrypted encrypt(byte[] data) throws GeneralSecurityException {
        if (state != State.AGREEMENT) {
            throw new IllegalStateException("no shared key");
        }
        SecretKey secret = new SecretKeySpec(derivedKey, "AES");
        Cipher cipher = encryptCipher;
        cipher.init(Cipher.ENCRYPT_MODE, secret);
        AlgorithmParameters params = cipher.getParameters();
        byte[] iv = params.getParameterSpec(IvParameterSpec.class).getIV();
        byte[] encrypted = cipher.doFinal(data);
//        byte[] sign = sign(data);
        return new Encrypted(iv, encrypted);
    }

    public byte[] decrypt(Encrypted encrypted) throws GeneralSecurityException {
        if (state != State.AGREEMENT) {
            throw new IllegalStateException("no shared key");
        }
        SecretKey secret = new SecretKeySpec(derivedKey, "AES");
        Cipher cipher = decryptCipher;
        cipher.init(Cipher.DECRYPT_MODE, secret, new IvParameterSpec(encrypted.iv()));
        byte[] decrypted = cipher.doFinal(encrypted.encrypted());
//        if (!verify(decrypted, encrypted.sign())) {
//            throw new IllegalArgumentException("not verified");
//        }
        return decrypted;
    }

    public boolean isAgreement() {
        return state == State.AGREEMENT;
    }

    public CompletableFuture<Void> agreementFuture() {
        return agreementFuture;
    }
}
