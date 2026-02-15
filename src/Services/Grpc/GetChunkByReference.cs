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
}

