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
            // Check if request was cancelled (common during replication timeouts)
            if (context.CancellationToken.IsCancellationRequested)
            {
                return new StoreChunkByKey_Res
                {
                    Success = false,
                    ErrorMessage = "Request cancelled"
                };
            }

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
        catch (TaskCanceledException) when (context.CancellationToken.IsCancellationRequested)
        {
            // Client cancelled the request (expected during replication timeouts) - suppress error
            return new StoreChunkByKey_Res
            {
                Success = false,
                ErrorMessage = "Request cancelled"
            };
        }
        catch (OperationCanceledException) when (context.CancellationToken.IsCancellationRequested)
        {
            // Client cancelled the request (expected during replication timeouts) - suppress error
            return new StoreChunkByKey_Res
            {
                Success = false,
                ErrorMessage = "Request cancelled"
            };
        }
        catch (Exception ex) when (ex.InnerException is Microsoft.AspNetCore.Connections.ConnectionAbortedException)
        {
            // HTTP/2 stream was reset by client (expected during replication) - suppress error
            return new StoreChunkByKey_Res
            {
                Success = false,
                ErrorMessage = "Connection aborted"
            };
        }
        catch (Exception ex)
        {
            // Only log unexpected errors (not cancellations)
            Console.WriteLine($"[StoreChunkByKey] Error for chunk_key={request.ChunkKey}: {ex.Message}");
            return new StoreChunkByKey_Res
            {
                Success = false,
                ErrorMessage = ex.Message
            };
        }
    }
}

