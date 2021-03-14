package song.pan.etl.common.util;

import song.pan.etl.common.exception.SystemException;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

public class AESUtils {


    static Cipher getCipher(String seed, int mode) throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException {
        if (seed == null) {
            throw new IllegalArgumentException("seed not specified");
        }
        KeyGenerator keygen = KeyGenerator.getInstance("AES");
        SecureRandom secureRandom = SecureRandom.getInstance("SHA1PRNG");
        secureRandom.setSeed(seed.getBytes());
        keygen.init(128, secureRandom);
        SecretKey secretKey = keygen.generateKey();
        byte[] encoded = secretKey.getEncoded();
        SecretKeySpec secretKeySpec = new SecretKeySpec(encoded, "AES");
        Cipher cipher = Cipher.getInstance("AES");
        cipher.init(mode, secretKeySpec);
        return cipher;
    }


    public static String encrypt(String seed, String src) {
        try {
            Cipher cipher = getCipher(seed, Cipher.ENCRYPT_MODE);
            byte[] bytes = cipher.doFinal(src.getBytes("utf-8"));
            return bytes2Hex(bytes);
        } catch (Exception e) {
            throw new SystemException(e);
        }
    }


    public static String decrypt(String seed, String src) {
        try {
            Cipher cipher = getCipher(seed, Cipher.DECRYPT_MODE);
            byte[] bytes = cipher.doFinal(hex2Bytes(src));
            return new String(bytes);
        } catch (Exception e) {
            throw new SystemException(e);
        }
    }


    static String bytes2Hex(byte[] buf) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < buf.length; i++) {
            String hex = Integer.toHexString(buf[i] & 0xFF);
            if (hex.length() == 1) {
                hex = '0' + hex;
            }
            sb.append(hex.toUpperCase());
        }
        return sb.toString();
    }


    static byte[] hex2Bytes(String hex) {
        if (hex.length() < 1) {
            return new byte[]{};
        }
        byte[] bytes = new byte[hex.length() / 2];
        for (int i = 0; i < hex.length() / 2; i++) {
            int high = Integer.parseInt(hex.substring(i * 2, i * 2 + 1), 16);
            int low = Integer.parseInt(hex.substring(i * 2 + 1, i * 2 + 2), 16);
            bytes[i] = (byte) (high * 16 + low);
        }
        return bytes;
    }

}
