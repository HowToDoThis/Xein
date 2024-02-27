using System.Security.Cryptography;
using System.Text;

namespace Xein.SDK.Newebpay;

public class Crypto
{
    public static byte[] EncryptAES(byte[] plainBytes, byte[] key, byte[] iv)
    {
        using var aes = Aes.Create();
        
        aes.Key  = key;
        aes.IV   = iv;
        aes.Mode = CipherMode.CBC;

        var encryptor = aes.CreateEncryptor(aes.Key, aes.IV);

        using var stream = new MemoryStream();
        using (var cs = new CryptoStream(stream, encryptor, CryptoStreamMode.Write, true))
            cs.Write(plainBytes, 0, plainBytes.Length);

        return stream.ToArray();
    }

    public static byte[] EncryptSHA(byte[] plainBytes)
    {
        return SHA256.HashData(plainBytes);
    }
}
