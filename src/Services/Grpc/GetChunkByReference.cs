using Agent.Modules.Storage;
using Grpc.Core;
using Google.Protobuf;

public class ChunkReferenceServiceImpl : ChunkReferenceService.ChunkReferenceServiceBase
{
    public override async Task<GetChunkByReference_Res> GetChunkByReference(
        GetChunkByReference_Req request,
        ServerCallContext context)
    {
        try
        {
            byte[]? chunk = await NetworkFileStorageHandler.GetChunkByReferenceAsync(
                request.BucketId,
                request.BucketIndex);

            if (chunk == null || chunk.Length == 0)
            {
                return new GetChunkByReference_Res
                {
                    Found = false,
                    Chunk = ByteString.Empty
                };
            }

            return new GetChunkByReference_Res
            {
                Found = true,
                Chunk = ByteString.CopyFrom(chunk)
            };
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[GetChunkByReference] Error for ({request.BucketId},{request.BucketIndex}): {ex.Message}");
            return new GetChunkByReference_Res
            {
                Found = false,
                Chunk = ByteString.Empty
            };
        }
    }

    public override async Task<GetChunkByKey_Res> GetChunkByKey(
        GetChunkByKey_Req request,
        ServerCallContext context)
    {
        try
        {
            byte[]? chunk = await NetworkFileStorageHandler.GetChunkAsync(request.ChunkKey);

            if (chunk == null || chunk.Length == 0)
            {
                return new GetChunkByKey_Res
                {
                    Found = false,
                    Chunk = ByteString.Empty
                };
            }

            return new GetChunkByKey_Res
            {
                Found = true,
                Chunk = ByteString.CopyFrom(chunk)
            };
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[GetChunkByKey] Error for chunk_key={request.ChunkKey}: {ex.Message}");
            return new GetChunkByKey_Res
            {
                Found = false,
                Chunk = ByteString.Empty
            };
        }
    }

    public override async Task<StoreChunkByKey_Res> StoreChunkByKey(
        StoreChunkByKey_Req request,
        ServerCallContext context)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(request.ChunkKey) || request.ChunkData == null || request.ChunkData.Length == 0)
            {
                return new StoreChunkByKey_Res
                {
                    Success = false,
                    ErrorMessage = "Invalid request: chunk_key or chunk_data is empty"
                };
            }

            // Verify chunk key matches data
            var expectedKey = NetworkFileStorageHandler.GenerateChunkKey(request.ChunkData.ToByteArray());
            if (expectedKey != request.ChunkKey)
            {
                return new StoreChunkByKey_Res
                {
                    Success = false,
                    ErrorMessage = $"Chunk key mismatch: expected {expectedKey}, got {request.ChunkKey}"
                };
            }

            // Store the chunk
            await NetworkFileStorageHandler.StoreChunkByKeyAsync(request.ChunkKey, request.ChunkData.ToByteArray());

            return new StoreChunkByKey_Res
            {
                Success = true
            };
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[StoreChunkByKey] Error for chunk_key={request.ChunkKey}: {ex.Message}");
            return new StoreChunkByKey_Res
            {
                Success = false,
                ErrorMessage = ex.Message
            };
        }
    }
}

