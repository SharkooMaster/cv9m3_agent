
using System.Numerics;
using System.Security.Cryptography;
using System.Text;
using Google.Protobuf.Reflection;

namespace Agent.Utils;

public static class NodeUtils
{
    public static uint GenerateNodeId(string ip, int port)
    {
        // Combine IP and port in a stable way
        string idString = $"{ip}:{port}";

        // Compute SHA-256 hash
        using (SHA256 sha256 = SHA256.Create())
        {
            byte[] hash = sha256.ComputeHash(Encoding.UTF8.GetBytes(idString));

            // Take the first 4 bytes (32 bits) for our address space
            uint firstFourBytes = BitConverter.ToUInt32(hash, 0);

            return firstFourBytes;
        }
    }

    public static Vector2 calculate_vector_coordinates(float[] _vector, string _binary_signature = "")
    {
        // Binary string based on the incoming vector. (i in vector -> i < 0 ? '0' : '1')
        string binary_signature = _binary_signature;
        if(binary_signature == "")
        {
            binary_signature = Misc.Misc.vector_to_bitstring(_vector);
        }

        // Dividing the 128 bit string into two 64 bit strings
        string x_coord_binary = binary_signature[..64];
        string y_coord_binary = binary_signature[64..];

        // Extracting 64 bit INT from each string
        float x_coord = Convert.ToInt64(x_coord_binary, 2);
        float y_coord = Convert.ToInt64(y_coord_binary, 2);

        return new Vector2(x_coord, y_coord);
    }

    public static string[] get_neighbor_buckets(string _bucket_id)
    {
        string[] to_return = new string[_bucket_id.Length];

        for (int i = 0; i < _bucket_id.Length; i++)
        {
            char[] n_bucket_id = _bucket_id.ToCharArray();
            n_bucket_id[i] = (n_bucket_id[i] == '0') ? '1' : '0';

            string n_bucket_id_str = new string(n_bucket_id);
            if(n_bucket_id_str != _bucket_id)
            {
                to_return[i] = n_bucket_id_str;
            }
        }
        return to_return;
    }

    public static int HashToRingPosition(string key, int m)
    {
        using (SHA256 sha256 = SHA256.Create())
        {
            byte[] hashBytes = sha256.ComputeHash(Encoding.UTF8.GetBytes(key));

            var hashValue = new System.Numerics.BigInteger(hashBytes.Reverse().ToArray());

            return (int)(hashValue % (1 << m));
        }
    }

    public static bool is_between(int pos, int s, int e)
    {
        if(s <= e)
        {
            return s < pos && pos <= e;
        }
        return pos > s || pos <= e;
    }
}