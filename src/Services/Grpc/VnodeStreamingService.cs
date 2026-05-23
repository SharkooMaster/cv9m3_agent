using System.Collections.Concurrent;
using System.Text;
using Agent.Modules.Storage;
using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;

namespace Agent.Services.Grpc;

/// <summary>
/// Vnode-data streaming RPC server, implementing the rebalance protocol
/// on the agent side. See agent/Protos/StreamVnodeData.proto for the
/// full protocol description.
///
/// Three responsibilities:
///   1. <see cref="StreamVnodeData"/> (server-streaming) — when this
///      agent is the *source* of a vnode handoff, iterate the local
///      chunk-store and stream every chunk to the dst peer. Filtering
///      by hash range is left to the dst (Phase 4 v1: dst persists
///      everything; range filtering arrives in v1.1).
///
///   2. <see cref="BeginVnodeAdoption"/> — when this agent is the *dst*
///      of a handoff, kick off a background pull from the named source
///      peer. Returns immediately with an adoption_id; progress is
///      observable via GetAdoptionStatus.
///
///   3. <see cref="GetAdoptionStatus"/> — small status probe so the
///      coordinator (gateway) can poll progress and detect stuck
///      adoptions without keeping a streaming connection open.
/// </summary>
public class VnodeStreamingGrpcService : VnodeStreamingService.VnodeStreamingServiceBase
{
    private const int MaxBatchEntries = 256;        // tune: balances per-batch cost vs streaming cadence
    private const long MaxBatchBytes = 8 * 1024 * 1024; // 8MB per batch — keeps gRPC frames under the 100MB default cap

    /// <summary>
    /// Active adoption tasks keyed by their opaque id. Bounded by the
    /// number of in-flight rebalance operations the coordinator triggers
    /// — for a typical 5-agent cluster scaling to 6, that's at most 1.
    /// </summary>
    private static readonly ConcurrentDictionary<string, AdoptionState> _adoptions = new();

    public override async Task StreamVnodeData(
        StreamVnodeData_Req request,
        IServerStreamWriter<StreamVnodeData_Batch> responseStream,
        ServerCallContext context)
    {
        Console.WriteLine(
            $"[VnodeStreamingService] StreamVnodeData requested by {request.RequestingPod} " +
            $"hash range [{request.HashRangeLow:x8},{request.HashRangeHigh:x8}]");

        // Refuse self-streams: the rebalance coordinator should never ask
        // an agent to stream to itself, but a stale dispatch (topology
        // already settled) could result in this. Cheap to detect, cheap
        // to refuse — and avoids a self-replicating loop bug.
        var myPod = Environment.GetEnvironmentVariable("MY_POD_NAME") ?? string.Empty;
        if (!string.IsNullOrEmpty(myPod) &&
            string.Equals(myPod, request.RequestingPod, StringComparison.Ordinal))
        {
            throw new RpcException(new Status(StatusCode.FailedPrecondition, "self-stream rejected"));
        }

        var batch = new StreamVnodeData_Batch();
        long bytesInBatch = 0;
        ulong cumulative = 0;

        foreach (var (storageGuid, chunkBytes) in NetworkFileStorageHandler.EnumerateAllChunks())
        {
            if (context.CancellationToken.IsCancellationRequested) break;

            batch.Entries.Add(new VnodeChunkEntry
            {
                StorageGuid = storageGuid,
                Chunk = ByteString.CopyFrom(chunkBytes),
                // Bitstring/Vector are best-effort omitted in v1 — the
                // dst's bucket index is rebuilt from incoming writes
                // and (Phase 5) anti-entropy. Shipping chunk bytes alone
                // is sufficient for content-addressable durability.
                Bitstring = string.Empty
            });
            bytesInBatch += chunkBytes.Length + 64; // 64 = storage_guid hex
            cumulative++;

            if (batch.Entries.Count >= MaxBatchEntries || bytesInBatch >= MaxBatchBytes)
            {
                batch.CumulativeStreamed = cumulative;
                batch.IsFinal = false;
                await responseStream.WriteAsync(batch);
                batch = new StreamVnodeData_Batch();
                bytesInBatch = 0;
            }
        }

        // Final batch carries the IsFinal flag so dst knows when the
        // stream completed cleanly vs. mid-stream truncation.
        batch.CumulativeStreamed = cumulative;
        batch.IsFinal = true;
        await responseStream.WriteAsync(batch);

        Console.WriteLine($"[VnodeStreamingService] StreamVnodeData → {request.RequestingPod} streamed {cumulative} chunks");
    }

    public override Task<BeginVnodeAdoption_Res> BeginVnodeAdoption(
        BeginVnodeAdoption_Req request,
        ServerCallContext context)
    {
        Console.WriteLine(
            $"[VnodeStreamingService] BeginVnodeAdoption from {request.SourcePod} ({request.SourceIp}) " +
            $"range [{request.HashRangeLow:x8},{request.HashRangeHigh:x8}] topo_v={request.TopologyVersion}");

        if (string.IsNullOrWhiteSpace(request.SourceIp))
        {
            return Task.FromResult(new BeginVnodeAdoption_Res
            {
                Accepted = false,
                AdoptionId = string.Empty,
                RejectReason = "source_ip is required"
            });
        }

        var adoptionId = Guid.NewGuid().ToString("N");
        var state = new AdoptionState
        {
            SourcePod = request.SourcePod ?? string.Empty,
            SourceIp = request.SourceIp,
            HashRangeLow = request.HashRangeLow,
            HashRangeHigh = request.HashRangeHigh,
            TopologyVersion = request.TopologyVersion,
            Status = AdoptionStatus.Running,
            ChunksStreamed = 0,
            StartedUtc = DateTime.UtcNow
        };
        _adoptions[adoptionId] = state;

        // Background work: open the StreamVnodeData call and ingest.
        // We don't await this — the RPC response goes back immediately
        // with the adoption_id so the coordinator can move on.
        _ = Task.Run(() => RunAdoptionAsync(adoptionId, state, context.CancellationToken));

        return Task.FromResult(new BeginVnodeAdoption_Res
        {
            Accepted = true,
            AdoptionId = adoptionId,
            RejectReason = string.Empty
        });
    }

    public override Task<GetVnodeMerkle_Res> GetVnodeMerkle(
        GetVnodeMerkle_Req request,
        ServerCallContext context)
    {
        // Bucket layout: 256 buckets indexed by storage_guid[0] (the
        // first hex character of the SHA256 → high nibble of byte 0,
        // packed into a uint8 0..255). For each bucket, we hash the
        // sorted concat of all storage_guid hex strings that landed in
        // it. Two replicas with the same set of chunks produce the
        // same 8 KB summary. Any discrepancy localises which bucket(s)
        // diverged; the caller drills in by re-issuing GetVnodeMerkle
        // (or directly triggering StreamVnodeData) on the differing
        // bucket's hash sub-range.
        const int Buckets = 256;
        var bucketGuids = new List<string>[Buckets];
        for (int i = 0; i < Buckets; i++) bucketGuids[i] = new List<string>();

        ulong total = 0;
        foreach (var (storageGuid, _) in NetworkFileStorageHandler.EnumerateAllChunks())
        {
            if (string.IsNullOrEmpty(storageGuid) || storageGuid.Length < 2) continue;
            // First two hex characters → first byte → bucket 0..255.
            // Cheap, deterministic, distributed uniformly because the
            // storage_guid is a SHA256 hex.
            int bucket;
            try
            {
                bucket = Convert.ToInt32(storageGuid.Substring(0, 2), 16);
            }
            catch
            {
                continue;
            }
            bucketGuids[bucket].Add(storageGuid);
            total++;
        }

        var summary = new byte[Buckets * 32];
        Span<byte> summarySpan = summary;
        for (int i = 0; i < Buckets; i++)
        {
            var list = bucketGuids[i];
            if (list.Count == 0) continue;

            // Sort so two replicas hashing the same set produce the
            // same digest regardless of insertion order on each side.
            list.Sort(StringComparer.Ordinal);

            using var sha = System.Security.Cryptography.SHA256.Create();
            // Avoid allocating one big concatenated string by hashing
            // each guid separately and chaining; final digest is the
            // SHA256 of the concatenation, which the receiver doesn't
            // need to know — only that "we agree iff our lists match".
            var sb = new StringBuilder(list.Count * 65);
            foreach (var g in list)
            {
                sb.Append(g);
                sb.Append('\n');
            }
            var bytes = Encoding.UTF8.GetBytes(sb.ToString());
            var hash = sha.ComputeHash(bytes);
            hash.AsSpan().CopyTo(summarySpan.Slice(i * 32, 32));
        }

        return Task.FromResult(new GetVnodeMerkle_Res
        {
            Summary = ByteString.CopyFrom(summary),
            ChunkCount = total
        });
    }

    public override Task<GetAdoptionStatus_Res> GetAdoptionStatus(
        GetAdoptionStatus_Req request,
        ServerCallContext context)
    {
        if (!_adoptions.TryGetValue(request.AdoptionId, out var state))
        {
            return Task.FromResult(new GetAdoptionStatus_Res
            {
                Status = GetAdoptionStatus_Res.Types.Status.Unknown,
                ChunksStreamed = 0,
                ErrorMessage = "unknown adoption_id"
            });
        }

        return Task.FromResult(new GetAdoptionStatus_Res
        {
            Status = state.Status switch
            {
                AdoptionStatus.Running => GetAdoptionStatus_Res.Types.Status.Running,
                AdoptionStatus.Completed => GetAdoptionStatus_Res.Types.Status.Completed,
                AdoptionStatus.Failed => GetAdoptionStatus_Res.Types.Status.Failed,
                _ => GetAdoptionStatus_Res.Types.Status.Unknown
            },
            ChunksStreamed = state.ChunksStreamed,
            ErrorMessage = state.ErrorMessage ?? string.Empty
        });
    }

    private static async Task RunAdoptionAsync(string adoptionId, AdoptionState state, CancellationToken outerCt)
    {
        try
        {
            // Direct dial to the source agent's gRPC endpoint. We don't
            // go through any pooled channel cache here because rebalance
            // is rare and the channel is short-lived (open → drain →
            // close once the stream completes).
            var address = state.SourceIp.Contains("://") ? state.SourceIp : $"http://{state.SourceIp}:5000";
            using var channel = GrpcChannel.ForAddress(address, new GrpcChannelOptions
            {
                Credentials = global::Grpc.Core.ChannelCredentials.Insecure,
                MaxReceiveMessageSize = 256 * 1024 * 1024,
                MaxSendMessageSize = 256 * 1024 * 1024
            });
            var client = new VnodeStreamingService.VnodeStreamingServiceClient(channel);

            var myPod = Environment.GetEnvironmentVariable("MY_POD_NAME") ?? string.Empty;
            var streamReq = new StreamVnodeData_Req
            {
                HashRangeLow = state.HashRangeLow,
                HashRangeHigh = state.HashRangeHigh,
                RequestingPod = myPod
            };

            using var call = client.StreamVnodeData(streamReq, cancellationToken: outerCt);
            await foreach (var batch in call.ResponseStream.ReadAllAsync(outerCt))
            {
                foreach (var entry in batch.Entries)
                {
                    if (string.IsNullOrEmpty(entry.StorageGuid) || entry.Chunk == null) continue;

                    // Re-store under the chunk's storage_guid. The agent's
                    // StoreChunkByKey verifies SHA256 match before
                    // persisting, so any tampering or wire-corruption is
                    // caught here.
                    var bytes = entry.Chunk.ToByteArray();
                    try
                    {
                        await NetworkFileStorageHandler.StoreChunkByKeyAsync(entry.StorageGuid, bytes);
                        state.ChunksStreamed++;
                    }
                    catch (Exception ex)
                    {
                        // Drop a single bad chunk; keep the stream going.
                        // Anti-entropy will repair the gap later.
                        Console.WriteLine($"[VnodeStreamingService] adoption {adoptionId} dropped chunk {entry.StorageGuid}: {ex.Message}");
                    }
                }

                if (batch.IsFinal) break;
            }

            state.Status = AdoptionStatus.Completed;
            Console.WriteLine($"[VnodeStreamingService] adoption {adoptionId} completed: {state.ChunksStreamed} chunks");
        }
        catch (OperationCanceledException) when (outerCt.IsCancellationRequested)
        {
            state.Status = AdoptionStatus.Failed;
            state.ErrorMessage = "cancelled";
        }
        catch (Exception ex)
        {
            state.Status = AdoptionStatus.Failed;
            state.ErrorMessage = $"{ex.GetType().Name}: {ex.Message}";
            Console.WriteLine($"[VnodeStreamingService] adoption {adoptionId} failed: {state.ErrorMessage}");
        }
    }

    private enum AdoptionStatus { Running, Completed, Failed }

    private sealed class AdoptionState
    {
        public string SourcePod = string.Empty;
        public string SourceIp = string.Empty;
        public uint HashRangeLow;
        public uint HashRangeHigh;
        public ulong TopologyVersion;
        public AdoptionStatus Status;
        public ulong ChunksStreamed;
        public string? ErrorMessage;
        public DateTime StartedUtc;
    }
}
