package xyz.binaryflux.jumpserver;

import java.io.*;
import java.nio.file.*;
import java.security.*;
import java.security.cert.*;
import java.security.spec.*;
import java.util.Base64;

public class PemUtil {

    public static X509Certificate loadCert(String pemPath) throws Exception {
        String pem = Files.readString(Path.of(pemPath));
        pem = pem.replaceAll("-----BEGIN CERTIFICATE-----", "")
                 .replaceAll("-----END CERTIFICATE-----", "")
                 .replaceAll("\\s", "");
        byte[] der = Base64.getDecoder().decode(pem);
        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        return (X509Certificate) cf.generateCertificate(new ByteArrayInputStream(der));
    }

    public static PrivateKey loadPrivateKey(String pemPath) throws Exception {
        String pem = Files.readString(Path.of(pemPath));
        pem = pem.replaceAll("-----BEGIN (?:RSA )?PRIVATE KEY-----", "")
                 .replaceAll("-----END (?:RSA )?PRIVATE KEY-----", "")
                 .replaceAll("\\s", "");
        byte[] der = Base64.getDecoder().decode(pem);

        PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(der);
        try {
            return KeyFactory.getInstance("RSA").generatePrivate(keySpec);
        } catch (InvalidKeySpecException e) {
            return KeyFactory.getInstance("EC").generatePrivate(keySpec);
        }
    }
}
