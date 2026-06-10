using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.IO.MemoryMappedFiles;
using System.Numerics;
using System.Runtime.InteropServices;

namespace Agent.Services.Index;

/// <summary>
/// ── BinaryVectorIndex: the agent's primary LSH search structure ──
///
/// Replaces RocksDB-iterator probing (L1 off) and the M_Bucket object cache
/// (L1 on) on the search hot path. Three design rules, in order:
///
///   A. The data plane lives OFF the GC object graph. All vector data is held
///      in a handful of large flat primitive arrays (hot tier) or memory-mapped
///      files (sealed tier). There are zero per-vector heap objects, so the GC
///      has nothing to fragment and nothing to trace — regardless of how many
///      hundreds of millions of vectors the agent holds.
///
///   B. Search is two-stage. Stage 1 probes 65 exact 64-bit codes (the query's
///      LSH code + its 64 single-bit flips — identical recall semantics to the
///      legacy neighbor-bucket probing) against in-RAM structures: a flat
///      hash multimap for the hot tier and a sorted-codes binary search for
///      sealed tiers. Stage 2 runs SIMD cosine only on the handful of
///      candidates stage 1 produced. RAM cost of stage 1 is 8 bytes/vector
///      for sealed data — the index stays RAM-resident essentially forever.
///
///   C. Segments are immutable once sealed. The hot segment is append-only;
///      when full it is frozen, sorted by code, written to one flat file, and
///      reopened as a memory-mapped sealed segment (codes stay in RAM, the
///      296-byte records are touched only for stage-2 candidates via the page
///      cache). Compaction is concatenation; there is no in-place mutation.
///
/// Identity facts this file relies on (verified against RocksDbBucketStorage
/// and cross/Misc.ComputeBitStringFromVector):
///   • bucketId == BitstringToUlong(bitstring): bit i of the string is bit i
///     of the ulong (LSB-first).
///   • bitstring char i == ('0' if vector[i] &lt; 0 else '1'). Therefore the
///     exact query code is recomputable from the query vector's signs and no
///     protocol change is needed.
///   • storageGuid is a 64-char lowercase hex SHA256 → packed to 32 raw bytes.
///
/// Durability: RocksDB (bv:/bsg:/bnext: keys) remains the source of truth.
/// This index is a derived structure — rebuilt from a full RocksDB scan at
/// startup, so it can never drift from storage across restarts. Sealed files
/// are process-lifetime artifacts (deleted and rebuilt on boot).
/// </summary>
public static class BinaryVectorIndex
{
    // ── Configuration (env) ──
    public static bool Enabled { get; private set; }
    private static int _segmentCapacity = 2_000_000;
    private static string _dir = "/data/chunks/binindex";
    private static int _vecDim = 64;

    // ── Tiers ──
    private static HotSegment _hot = null!;
    // Snapshot arrays are replaced wholesale under _tierLock; readers take
    // Volatile.Read and never observe partial updates.
    private static HotSegment[] _frozen = Array.Empty<HotSegment>();
    private static SealedSegment[] _sealed = Array.Empty<SealedSegment>();

    private static readonly object _appendLock = new();
    private static readonly object _tierLock = new();
    private static long _totalEntries;
    private static int _pendingSeals;
    private static long _sealFailures;

    // Background merge: keep sealed-segment count low so probe cost stays
    // O(fanin·log n) instead of growing linearly with ingested data.
    private static int _mergeFanIn = 8;
    private static int _mergeRunning;

    public readonly record struct Hit(ulong BucketId, ulong BucketIndex, string StorageGuid, float Similarity);

    // ══════════════════════════════════════════════════════════════
    //  INITIALIZATION
    // ══════════════════════════════════════════════════════════════

    public static void Initialize()
    {
        Enabled = (Environment.GetEnvironmentVariable("BINARY_INDEX_ENABLED") ?? "true")
            .Trim().ToLowerInvariant() is "1" or "true" or "yes" or "on";

        if (int.TryParse(Environment.GetEnvironmentVariable("BINARY_INDEX_SEGMENT_CAPACITY"), out var cap) && cap >= 4096)
            _segmentCapacity = cap;

        if (int.TryParse(Environment.GetEnvironmentVariable("BINARY_INDEX_MERGE_FANIN"), out var fanIn) && fanIn >= 2)
            _mergeFanIn = fanIn;

        var dirEnv = Environment.GetEnvironmentVariable("BINARY_INDEX_DIR");
        if (!string.IsNullOrWhiteSpace(dirEnv))
        {
            _dir = dirEnv;
        }
        else
        {
            // Default: sibling of the RocksDB directory (same volume).
            var rocksPath = Environment.GetEnvironmentVariable("ROCKSDB_PATH") ?? "/data/chunks/rocksdb";
            _dir = Path.Combine(Path.GetDirectoryName(rocksPath.TrimEnd('/')) ?? "/data/chunks", "binindex");
        }

        if (!Enabled)
        {
            Console.WriteLine("[BinaryIndex] DISABLED (BINARY_INDEX_ENABLED=false) — legacy search paths in effect");
            return;
        }

        // Segments are derived data rebuilt from RocksDB each boot; stale files
        // from a previous process are unreferenced garbage. Start clean.
        try { if (Directory.Exists(_dir)) Directory.Delete(_dir, recursive: true); } catch { }
        Directory.CreateDirectory(_dir);

        _hot = new HotSegment(_segmentCapacity, _vecDim);
        Console.WriteLine($"[BinaryIndex] ENABLED dir={_dir} segmentCapacity={_segmentCapacity:N0}");
    }

    /// <summary>
    /// Recompute the exact 64-bit LSH code from a query vector's signs.
    /// Must match cross's ComputeBitStringFromVector ('0' iff v[i] &lt; 0)
    /// composed with BitstringToUlong (string char i → ulong bit i).
    /// </summary>
    public static ulong CodeFromVector(ReadOnlySpan<float> vector)
    {
        ulong code = 0;
        int n = Math.Min(64, vector.Length);
        for (int i = 0; i < n; i++)
            if (!(vector[i] < 0f))
                code |= 1UL << i;
        return code;
    }

    // ══════════════════════════════════════════════════════════════
    //  APPEND (store path)
    // ══════════════════════════════════════════════════════════════

    /// <summary>
    /// Append one freshly stored vector. Called from RocksDbBucketStorage.StoreVector
    /// — the single funnel through which every bucket-vector write passes
    /// (client stores, L1 inserts, vnode adoption). Never called for dedup
    /// returns, so entries are unique per (bucketId, bucketIndex).
    /// </summary>
    public static void Append(ulong bucketId, ulong bucketIndex, float[] vector, string storageGuid, int chunkSize)
    {
        if (!Enabled) return;

        float normSq = 0f;
        for (int i = 0; i < vector.Length; i++) normSq += vector[i] * vector[i];

        lock (_appendLock)
        {
            if (!_hot.TryAppend(bucketId, bucketIndex, vector, storageGuid, normSq))
            {
                // Hot tier full: freeze it (stays searchable as-is) and start a
                // fresh hot segment immediately so the store path never stalls
                // on the seal. The freeze→seal conversion runs in the background.
                var frozen = _hot;
                lock (_tierLock)
                {
                    var nf = new HotSegment[_frozen.Length + 1];
                    Array.Copy(_frozen, nf, _frozen.Length);
                    nf[^1] = frozen;
                    Volatile.Write(ref _frozen, nf);
                }
                _hot = new HotSegment(_segmentCapacity, _vecDim);
                Interlocked.Increment(ref _pendingSeals);
                _ = Task.Run(() => SealFrozenSegment(frozen));

                bool ok = _hot.TryAppend(bucketId, bucketIndex, vector, storageGuid, normSq);
                if (!ok) throw new InvalidOperationException("[BinaryIndex] fresh hot segment rejected append");
            }
            Interlocked.Increment(ref _totalEntries);
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  SEARCH (two-stage)
    // ══════════════════════════════════════════════════════════════

    /// <summary>
    /// Probe the query code + its 64 single-bit flips (identical recall
    /// semantics to legacy neighbor-bucket probing), then cosine-score the
    /// candidates. Returns up to k hits with similarity ≥ threshold, best first.
    /// </summary>
    public static List<Hit> Search(ulong queryCode, float[] queryVector, float queryNormSq, float threshold, int k)
    {
        var hits = new List<Hit>(Math.Max(1, k));
        if (!Enabled || _totalEntries == 0) return hits;

        var hot = Volatile.Read(ref _hot);
        var frozen = Volatile.Read(ref _frozen);
        var sealedSegs = Volatile.Read(ref _sealed);

        Span<ulong> probes = stackalloc ulong[65];
        probes[0] = queryCode;
        for (int b = 0; b < 64; b++)
            probes[b + 1] = queryCode ^ (1UL << b);

        if (k <= 1)
        {
            float bestSim = -1f;
            ulong bestBucket = 0, bestIndex = 0;
            string? bestGuid = null;

            for (int p = 0; p < probes.Length; p++)
            {
                ulong code = probes[p];
                hot.ScoreProbe(code, queryVector, queryNormSq, threshold, ref bestSim, ref bestBucket, ref bestIndex, ref bestGuid);
                for (int f = 0; f < frozen.Length; f++)
                    frozen[f].ScoreProbe(code, queryVector, queryNormSq, threshold, ref bestSim, ref bestBucket, ref bestIndex, ref bestGuid);
                for (int s = 0; s < sealedSegs.Length; s++)
                    sealedSegs[s].ScoreProbe(code, queryVector, queryNormSq, threshold, ref bestSim, ref bestBucket, ref bestIndex, ref bestGuid);
            }

            if (bestGuid != null)
                hits.Add(new Hit(bestBucket, bestIndex, bestGuid, bestSim));
            return hits;
        }

        // k > 1 (high-entropy queries): collect everything ≥ threshold, then rank.
        var all = new List<Hit>(16);
        for (int p = 0; p < probes.Length; p++)
        {
            ulong code = probes[p];
            hot.CollectProbe(code, queryVector, queryNormSq, threshold, all);
            for (int f = 0; f < frozen.Length; f++)
                frozen[f].CollectProbe(code, queryVector, queryNormSq, threshold, all);
            for (int s = 0; s < sealedSegs.Length; s++)
                sealedSegs[s].CollectProbe(code, queryVector, queryNormSq, threshold, all);
        }
        all.Sort((a, b) => b.Similarity.CompareTo(a.Similarity));
        for (int i = 0; i < all.Count && i < k; i++)
            hits.Add(all[i]);
        return hits;
    }

    /// <summary>
    /// Store-time dedup probe: exact bucket only (Hamming 0). Replaces the
    /// per-store RocksDB prefix scan AND fixes its 5-second blind window
    /// (write-batcher lag): entries appended here are visible immediately.
    /// </summary>
    public static Hit? SearchExactBucket(ulong bucketId, float[] queryVector, float queryNormSq, float threshold)
    {
        if (!Enabled || _totalEntries == 0) return null;

        var hot = Volatile.Read(ref _hot);
        var frozen = Volatile.Read(ref _frozen);
        var sealedSegs = Volatile.Read(ref _sealed);

        float bestSim = -1f;
        ulong bestBucket = 0, bestIndex = 0;
        string? bestGuid = null;

        hot.ScoreProbe(bucketId, queryVector, queryNormSq, threshold, ref bestSim, ref bestBucket, ref bestIndex, ref bestGuid);
        for (int f = 0; f < frozen.Length; f++)
            frozen[f].ScoreProbe(bucketId, queryVector, queryNormSq, threshold, ref bestSim, ref bestBucket, ref bestIndex, ref bestGuid);
        for (int s = 0; s < sealedSegs.Length; s++)
            sealedSegs[s].ScoreProbe(bucketId, queryVector, queryNormSq, threshold, ref bestSim, ref bestBucket, ref bestIndex, ref bestGuid);

        return bestGuid != null ? new Hit(bestBucket, bestIndex, bestGuid, bestSim) : null;
    }

    // ══════════════════════════════════════════════════════════════
    //  REBUILD (startup)
    // ══════════════════════════════════════════════════════════════

    /// <summary>
    /// Populate from a full scan of the bucket RocksDB. Called once at startup
    /// before the gRPC server begins serving, so no appends race the rebuild.
    /// </summary>
    public static void RebuildFrom(Agent.Services.Storage.RocksDbBucketStorage bucketStorage)
    {
        if (!Enabled) return;

        var sw = System.Diagnostics.Stopwatch.StartNew();
        long n = bucketStorage.EnumerateAllVectorRecords((bucketId, index, vector, guid, normSq, chunkSize) =>
        {
            Append(bucketId, index, vector, guid, chunkSize);
        });
        sw.Stop();
        Console.WriteLine($"[BinaryIndex] Rebuilt {n:N0} entries from RocksDB in {sw.ElapsedMilliseconds:N0}ms " +
                          $"(hot={_hot.Count:N0}, frozen={_frozen.Length}, sealed={_sealed.Length})");
    }

    // ══════════════════════════════════════════════════════════════
    //  STATS / TEST SUPPORT
    // ══════════════════════════════════════════════════════════════

    public static (long entries, int frozenCount, int sealedCount, long ramBytes) GetStats()
    {
        var hot = Volatile.Read(ref _hot);
        var frozen = Volatile.Read(ref _frozen);
        var sealedSegs = Volatile.Read(ref _sealed);

        long ram = hot?.RamBytes ?? 0;
        foreach (var f in frozen) ram += f.RamBytes;
        foreach (var s in sealedSegs) ram += s.RamBytes;
        return (Interlocked.Read(ref _totalEntries), frozen.Length, sealedSegs.Length, ram);
    }

    /// <summary>Block until all in-flight freeze→seal conversions and merges finish (tests).</summary>
    public static void WaitForSeals(int timeoutMs = 60_000)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        while ((Volatile.Read(ref _pendingSeals) > 0 || Volatile.Read(ref _mergeRunning) != 0)
               && sw.ElapsedMilliseconds < timeoutMs)
            Thread.Sleep(10);
    }

    /// <summary>Tear down and re-init with current env (tests only).</summary>
    public static void ResetForTests()
    {
        lock (_appendLock)
        lock (_tierLock)
        {
            foreach (var s in _sealed) s.Dispose();
            _sealed = Array.Empty<SealedSegment>();
            _frozen = Array.Empty<HotSegment>();
            _totalEntries = 0;
            _sealFailures = 0;
            Initialize();
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  SEAL: frozen hot segment → sorted mmap file
    // ══════════════════════════════════════════════════════════════

    private static void SealFrozenSegment(HotSegment frozen)
    {
        try
        {
            string path = Path.Combine(_dir, $"seg-{Guid.NewGuid():N}.bvix");
            var sealedSeg = SealedSegment.CreateFromHot(frozen, path);

            lock (_tierLock)
            {
                // Swap: add sealed, remove this frozen.
                var ns = new SealedSegment[_sealed.Length + 1];
                Array.Copy(_sealed, ns, _sealed.Length);
                ns[^1] = sealedSeg;

                var nf = new List<HotSegment>(_frozen.Length);
                foreach (var f in _frozen) if (!ReferenceEquals(f, frozen)) nf.Add(f);

                // Publish sealed BEFORE removing frozen: a racing reader may
                // score the same entry twice for one probe (harmless — equal
                // sim, same winner), but never misses it.
                Volatile.Write(ref _sealed, ns);
                Volatile.Write(ref _frozen, nf.ToArray());
            }
            Console.WriteLine($"[BinaryIndex] Sealed segment: {frozen.Count:N0} entries → {path}");
            MaybeScheduleMerge();
        }
        catch (Exception ex)
        {
            // Keep the frozen segment in RAM — still fully searchable, just
            // not converted to mmap. Log loudly; RAM growth is the only cost.
            Interlocked.Increment(ref _sealFailures);
            Console.WriteLine($"[BinaryIndex] SEAL FAILED ({ex.GetType().Name}): {ex.Message} — frozen segment retained in RAM");
        }
        finally
        {
            Interlocked.Decrement(ref _pendingSeals);
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  MERGE: many sealed segments → one (streaming k-way merge)
    // ══════════════════════════════════════════════════════════════

    private static void MaybeScheduleMerge()
    {
        if (Volatile.Read(ref _sealed).Length < _mergeFanIn) return;
        if (Interlocked.CompareExchange(ref _mergeRunning, 1, 0) != 0) return;
        _ = Task.Run(RunMerge);
    }

    private static void RunMerge()
    {
        try
        {
            while (true)
            {
                var parts = Volatile.Read(ref _sealed);
                if (parts.Length < _mergeFanIn) return;

                var sw = System.Diagnostics.Stopwatch.StartNew();
                string path = Path.Combine(_dir, $"seg-{Guid.NewGuid():N}.bvix");
                SealedSegment merged;
                try
                {
                    merged = SealedSegment.Merge(parts, path);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[BinaryIndex] MERGE FAILED ({ex.GetType().Name}): {ex.Message} — keeping {parts.Length} segments");
                    return;
                }

                lock (_tierLock)
                {
                    var current = _sealed;
                    // Keep any segments sealed after the merge snapshot was taken.
                    var keep = new List<SealedSegment>(current.Length - parts.Length + 1) { merged };
                    foreach (var s in current)
                        if (Array.IndexOf(parts, s) < 0)
                            keep.Add(s);
                    Volatile.Write(ref _sealed, keep.ToArray());
                }
                sw.Stop();
                Console.WriteLine($"[BinaryIndex] Merged {parts.Length} segments → {merged.Count:N0} entries in {sw.ElapsedMilliseconds:N0}ms");

                // Deferred reclamation: searches snapshot the segment array and
                // probe without refcounting, so give in-flight readers a generous
                // grace period before unmapping the merged-away files. Probes
                // complete in microseconds; 60s is overkill by design.
                var old = parts;
                _ = Task.Delay(TimeSpan.FromSeconds(60)).ContinueWith(_ =>
                {
                    foreach (var s in old)
                    {
                        try { s.Dispose(); } catch { }
                        try { if (s.FilePath != null) File.Delete(s.FilePath); } catch { }
                    }
                });
            }
        }
        finally
        {
            Volatile.Write(ref _mergeRunning, 0);
            // A seal may have pushed past the fan-in while we were finishing.
            MaybeScheduleMerge();
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  SIMD cosine (span-based; works over RAM arrays and mmap views)
    // ══════════════════════════════════════════════════════════════

    internal static float Cosine(ReadOnlySpan<float> q, float qNormSq, ReadOnlySpan<float> e, float eNormSq)
    {
        if (qNormSq <= 0f || eNormSq <= 0f || q.Length != e.Length) return -1f;

        float dot = 0f;
        int i = 0;
        if (Vector.IsHardwareAccelerated && q.Length >= Vector<float>.Count)
        {
            var acc = Vector<float>.Zero;
            int w = Vector<float>.Count;
            for (; i <= q.Length - w; i += w)
                acc += new Vector<float>(q.Slice(i, w)) * new Vector<float>(e.Slice(i, w));
            dot = Vector.Dot(acc, Vector<float>.One);
        }
        for (; i < q.Length; i++)
            dot += q[i] * e[i];

        return dot / MathF.Sqrt(qNormSq * eNormSq);
    }

    // ══════════════════════════════════════════════════════════════
    //  HOT SEGMENT: append-only flat arrays + striped probe multimap
    // ══════════════════════════════════════════════════════════════

    internal sealed class HotSegment
    {
        private readonly int _capacity;
        private readonly int _dim;
        private int _count;

        // Flat parallel arrays — a handful of large allocations, zero
        // per-entry objects. Entry i: codes[i], bucketIndexes[i],
        // normSqs[i], guids[i*32..], vectors[i*dim..].
        internal readonly ulong[] Codes;
        internal readonly ulong[] BucketIndexes;
        internal readonly float[] NormSqs;
        internal readonly byte[] Guids;
        internal readonly float[] Vectors;

        // Probe multimap: 256 stripes keyed by full code; chains through
        // _next (entry idx → older entry idx with the same code, -1 = end).
        // Chains only ever point to OLDER (fully written) entries, and the
        // stripe lock pairs release(writer)/acquire(reader), so chain walks
        // outside the lock are safe.
        private readonly Dictionary<ulong, int>[] _stripeHeads;
        private readonly object[] _stripeLocks;
        private readonly int[] _next;

        // Rare fallback for storage guids that are not 64-char hex (none in
        // practice — GenerateChunkKey always emits lowercase hex SHA256).
        private readonly ConcurrentDictionary<int, string> _oddGuids = new();

        public int Count => Volatile.Read(ref _count);

        public long RamBytes =>
            (long)_capacity * (8 + 8 + 4 + 32 + _dim * 4 + 4) + _stripeHeads.Length * 64L;

        public HotSegment(int capacity, int dim)
        {
            _capacity = capacity;
            _dim = dim;
            Codes = new ulong[capacity];
            BucketIndexes = new ulong[capacity];
            NormSqs = new float[capacity];
            Guids = new byte[(long)capacity * 32 <= int.MaxValue ? capacity * 32 : throw new ArgumentOutOfRangeException(nameof(capacity))];
            Vectors = new float[(long)capacity * dim <= int.MaxValue ? capacity * dim : throw new ArgumentOutOfRangeException(nameof(capacity))];
            _next = new int[capacity];
            _stripeHeads = new Dictionary<ulong, int>[256];
            _stripeLocks = new object[256];
            for (int i = 0; i < 256; i++)
            {
                _stripeHeads[i] = new Dictionary<ulong, int>();
                _stripeLocks[i] = new object();
            }
        }

        /// <summary>Caller holds the index-wide append lock.</summary>
        public bool TryAppend(ulong code, ulong bucketIndex, float[] vector, string guid, float normSq)
        {
            int idx = _count;
            if (idx >= _capacity) return false;

            Codes[idx] = code;
            BucketIndexes[idx] = bucketIndex;
            NormSqs[idx] = normSq;

            int n = Math.Min(vector.Length, _dim);
            Array.Copy(vector, 0, Vectors, idx * _dim, n);

            if (guid.Length == 64 && TryHexTo32(guid, Guids.AsSpan(idx * 32, 32)))
            {
                // packed
            }
            else
            {
                _oddGuids[idx] = guid;
            }

            int stripe = (int)(code & 0xFF);
            lock (_stripeLocks[stripe])
            {
                var heads = _stripeHeads[stripe];
                _next[idx] = heads.TryGetValue(code, out var head) ? head : -1;
                heads[code] = idx;
            }

            Volatile.Write(ref _count, idx + 1);
            return true;
        }

        public void ScoreProbe(ulong code, float[] q, float qNormSq, float threshold,
            ref float bestSim, ref ulong bestBucket, ref ulong bestIndex, ref string? bestGuid)
        {
            int idx = ProbeHead(code);
            while (idx >= 0)
            {
                float sim = Cosine(q, qNormSq, Vectors.AsSpan(idx * _dim, _dim), NormSqs[idx]);
                if (sim >= threshold && sim > bestSim)
                {
                    bestSim = sim;
                    bestBucket = Codes[idx];
                    bestIndex = BucketIndexes[idx];
                    bestGuid = GuidAt(idx);
                }
                idx = _next[idx];
            }
        }

        public void CollectProbe(ulong code, float[] q, float qNormSq, float threshold, List<Hit> sink)
        {
            int idx = ProbeHead(code);
            while (idx >= 0)
            {
                float sim = Cosine(q, qNormSq, Vectors.AsSpan(idx * _dim, _dim), NormSqs[idx]);
                if (sim >= threshold)
                    sink.Add(new Hit(Codes[idx], BucketIndexes[idx], GuidAt(idx), sim));
                idx = _next[idx];
            }
        }

        private int ProbeHead(ulong code)
        {
            int stripe = (int)(code & 0xFF);
            lock (_stripeLocks[stripe])
            {
                return _stripeHeads[stripe].TryGetValue(code, out var head) ? head : -1;
            }
        }

        internal string GuidAt(int idx) =>
            _oddGuids.TryGetValue(idx, out var odd)
                ? odd
                : Convert.ToHexString(Guids.AsSpan(idx * 32, 32)).ToLowerInvariant();

        internal string? OddGuidAt(int idx) => _oddGuids.TryGetValue(idx, out var odd) ? odd : null;

        private static bool TryHexTo32(string hex, Span<byte> dest)
        {
            for (int i = 0; i < 32; i++)
            {
                int hi = HexVal(hex[i * 2]);
                int lo = HexVal(hex[i * 2 + 1]);
                if (hi < 0 || lo < 0) return false;
                dest[i] = (byte)((hi << 4) | lo);
            }
            return true;

            static int HexVal(char c) => c switch
            {
                >= '0' and <= '9' => c - '0',
                >= 'a' and <= 'f' => c - 'a' + 10,
                >= 'A' and <= 'F' => c - 'A' + 10,
                _ => -1
            };
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  SEALED SEGMENT: sorted codes in RAM (8 B/vector), records mmap'd
    // ══════════════════════════════════════════════════════════════
    //
    //  File layout (little-endian):
    //    [ 0..  8) magic "BVIX0001"
    //    [ 8.. 16) long entryCount
    //    [16.. 20) int vecDim
    //    [20.. 24) int reserved
    //    [24..   ) ulong codes[count]          (sorted ascending)
    //              ulong bucketIndexes[count]
    //              float normSqs[count]
    //              byte  guids[count * 32]
    //              float vectors[count * dim]

    internal sealed class SealedSegment : IDisposable
    {
        private static readonly byte[] Magic = "BVIX0001"u8.ToArray();

        private readonly MemoryMappedFile _mmf;
        private readonly MemoryMappedViewAccessor _view;
        private readonly unsafe byte* _base;
        private readonly long _bucketIndexesOff, _normSqsOff, _guidsOff, _vectorsOff;
        private readonly int _count;
        private readonly int _dim;
        private readonly Dictionary<int, string>? _oddGuids;

        /// <summary>Sorted codes, RAM-resident: the only per-vector RAM this tier costs.</summary>
        private readonly ulong[] _codes;

        public int Count => _count;
        public long RamBytes => (long)_count * 8;
        public string? FilePath { get; private set; }

        private unsafe SealedSegment(MemoryMappedFile mmf, MemoryMappedViewAccessor view, ulong[] codes,
            int count, int dim, Dictionary<int, string>? oddGuids)
        {
            _mmf = mmf;
            _view = view;
            _codes = codes;
            _count = count;
            _dim = dim;
            _oddGuids = oddGuids;

            byte* ptr = null;
            view.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
            _base = ptr + view.PointerOffset;

            long headerLen = 24;
            long codesLen = (long)count * 8;
            _bucketIndexesOff = headerLen + codesLen;
            _normSqsOff = _bucketIndexesOff + (long)count * 8;
            _guidsOff = _normSqsOff + (long)count * 4;
            _vectorsOff = _guidsOff + (long)count * 32;
        }

        public static SealedSegment CreateFromHot(HotSegment hot, string path)
        {
            int count = hot.Count;
            int dim = hot.Vectors.Length / hot.Codes.Length;

            // Sort permutation by code → equal codes become one contiguous run,
            // probed with a single binary search.
            var perm = new int[count];
            for (int i = 0; i < count; i++) perm[i] = i;
            var keys = new ulong[count];
            Array.Copy(hot.Codes, keys, count);
            Array.Sort(keys, perm);

            long headerLen = 24;
            long fileLen = headerLen
                + (long)count * 8     // codes
                + (long)count * 8     // bucketIndexes
                + (long)count * 4     // normSqs
                + (long)count * 32    // guids
                + (long)count * dim * 4; // vectors

            Dictionary<int, string>? oddGuids = null;

            using (var fs = new FileStream(path, FileMode.CreateNew, FileAccess.Write, FileShare.None,
                       bufferSize: 1 << 20, FileOptions.SequentialScan))
            using (var bw = new BinaryWriter(fs))
            {
                bw.Write(Magic);
                bw.Write((long)count);
                bw.Write(dim);
                bw.Write(0);

                var u8 = new byte[8];
                for (int i = 0; i < count; i++) { BinaryPrimitives.WriteUInt64LittleEndian(u8, keys[i]); bw.Write(u8); }
                for (int i = 0; i < count; i++) { BinaryPrimitives.WriteUInt64LittleEndian(u8, hot.BucketIndexes[perm[i]]); bw.Write(u8); }
                var f4 = new byte[4];
                for (int i = 0; i < count; i++) { BinaryPrimitives.WriteSingleLittleEndian(f4, hot.NormSqs[perm[i]]); bw.Write(f4); }
                for (int i = 0; i < count; i++)
                {
                    bw.Write(hot.Guids, perm[i] * 32, 32);
                    var odd = hot.OddGuidAt(perm[i]);
                    if (odd != null)
                    {
                        oddGuids ??= new Dictionary<int, string>();
                        oddGuids[i] = odd;
                    }
                }
                var vecBytes = new byte[dim * 4];
                for (int i = 0; i < count; i++)
                {
                    Buffer.BlockCopy(hot.Vectors, perm[i] * dim * 4, vecBytes, 0, dim * 4);
                    bw.Write(vecBytes);
                }
                bw.Flush();
            }

            var mmf = MemoryMappedFile.CreateFromFile(path, FileMode.Open, mapName: null, capacity: 0,
                MemoryMappedFileAccess.Read);
            var view = mmf.CreateViewAccessor(0, fileLen, MemoryMappedFileAccess.Read);
            return new SealedSegment(mmf, view, keys, count, dim, oddGuids) { FilePath = path };
        }

        // ── Raw per-entry readers (merge support) ──
        internal ulong CodeAt(int i) => _codes[i];
        internal unsafe ulong BucketIndexAt(int i) => *(ulong*)(_base + _bucketIndexesOff + (long)i * 8);
        internal unsafe float NormSqAt(int i) => *(float*)(_base + _normSqsOff + (long)i * 4);
        internal unsafe ReadOnlySpan<byte> GuidBytesAt(int i) => new(_base + _guidsOff + (long)i * 32, 32);
        internal unsafe ReadOnlySpan<byte> VectorBytesAt(int i) => new(_base + _vectorsOff + (long)i * _dim * 4, _dim * 4);
        internal string? OddGuidAt(int i) => _oddGuids != null && _oddGuids.TryGetValue(i, out var s) ? s : null;
        internal int Dim => _dim;

        /// <summary>
        /// Streaming k-way merge of sorted sealed segments into one new segment.
        /// Sections are written DIRECTLY into the final file in layout order by
        /// replaying the merge selection once per section (5 cheap passes over
        /// the RAM-resident codes). No temp files → peak disk usage during a
        /// merge is old + new (2×), not 3× — this matters on small node disks
        /// where the original temp-file design triggered kubelet disk-pressure
        /// eviction. O(1) RAM beyond the merged codes array (which becomes the
        /// new segment's RAM-resident code index anyway).
        /// </summary>
        public static SealedSegment Merge(SealedSegment[] parts, string path)
        {
            long totalL = 0;
            int dim = parts[0].Dim;
            foreach (var p in parts)
            {
                if (p.Dim != dim) throw new InvalidOperationException("segment dim mismatch");
                totalL += p.Count;
            }
            if (totalL > int.MaxValue) throw new InvalidOperationException("merged segment too large");
            int total = (int)totalL;

            var codes = new ulong[total];
            Dictionary<int, string>? oddGuids = null;

            // Replays the k-way merge selection, invoking emit(part, entryIdx)
            // in merged (code-ascending) order. Selection runs entirely on the
            // RAM codes arrays; fan-in is small so the linear min-scan is fine.
            void MergePass(Action<SealedSegment, int> emit)
            {
                var pos = new int[parts.Length];
                for (int outIdx = 0; outIdx < total; outIdx++)
                {
                    int src = -1;
                    ulong minCode = ulong.MaxValue;
                    for (int p = 0; p < parts.Length; p++)
                    {
                        if (pos[p] >= parts[p].Count) continue;
                        ulong c = parts[p].CodeAt(pos[p]);
                        if (src < 0 || c < minCode) { src = p; minCode = c; }
                    }
                    emit(parts[src], pos[src]++);
                }
            }

            long fileLen;
            using (var fs = new FileStream(path, FileMode.CreateNew, FileAccess.Write, FileShare.None, 1 << 20))
            using (var bw = new BinaryWriter(fs))
            {
                bw.Write(Magic);
                bw.Write((long)total);
                bw.Write(dim);
                bw.Write(0);

                var u8 = new byte[8];
                var f4 = new byte[4];

                // Pass 1: codes (also fills the RAM array).
                {
                    int outIdx = 0;
                    MergePass((seg, i) =>
                    {
                        ulong c = seg.CodeAt(i);
                        codes[outIdx++] = c;
                        BinaryPrimitives.WriteUInt64LittleEndian(u8, c);
                        bw.Write(u8);
                    });
                }
                // Pass 2: bucket indexes.
                MergePass((seg, i) =>
                {
                    BinaryPrimitives.WriteUInt64LittleEndian(u8, seg.BucketIndexAt(i));
                    bw.Write(u8);
                });
                // Pass 3: norms.
                MergePass((seg, i) =>
                {
                    BinaryPrimitives.WriteSingleLittleEndian(f4, seg.NormSqAt(i));
                    bw.Write(f4);
                });
                // Pass 4: guids (collect odd-guid remaps here).
                {
                    int outIdx = 0;
                    MergePass((seg, i) =>
                    {
                        bw.Write(seg.GuidBytesAt(i));
                        var odd = seg.OddGuidAt(i);
                        if (odd != null)
                        {
                            oddGuids ??= new Dictionary<int, string>();
                            oddGuids[outIdx] = odd;
                        }
                        outIdx++;
                    });
                }
                // Pass 5: vectors.
                MergePass((seg, i) => bw.Write(seg.VectorBytesAt(i)));

                bw.Flush();
                fileLen = fs.Length;
            }

            var mmf = MemoryMappedFile.CreateFromFile(path, FileMode.Open, mapName: null, capacity: 0,
                MemoryMappedFileAccess.Read);
            var view = mmf.CreateViewAccessor(0, fileLen, MemoryMappedFileAccess.Read);
            return new SealedSegment(mmf, view, codes, total, dim, oddGuids) { FilePath = path };
        }

        public unsafe void ScoreProbe(ulong code, float[] q, float qNormSq, float threshold,
            ref float bestSim, ref ulong bestBucket, ref ulong bestIndex, ref string? bestGuid)
        {
            var (lo, hi) = EqualRange(code);
            for (int i = lo; i < hi; i++)
            {
                float eNormSq = *(float*)(_base + _normSqsOff + (long)i * 4);
                var eVec = new ReadOnlySpan<float>(_base + _vectorsOff + (long)i * _dim * 4, _dim);
                float sim = Cosine(q, qNormSq, eVec, eNormSq);
                if (sim >= threshold && sim > bestSim)
                {
                    bestSim = sim;
                    bestBucket = code;
                    bestIndex = *(ulong*)(_base + _bucketIndexesOff + (long)i * 8);
                    bestGuid = GuidAt(i);
                }
            }
        }

        public unsafe void CollectProbe(ulong code, float[] q, float qNormSq, float threshold, List<Hit> sink)
        {
            var (lo, hi) = EqualRange(code);
            for (int i = lo; i < hi; i++)
            {
                float eNormSq = *(float*)(_base + _normSqsOff + (long)i * 4);
                var eVec = new ReadOnlySpan<float>(_base + _vectorsOff + (long)i * _dim * 4, _dim);
                float sim = Cosine(q, qNormSq, eVec, eNormSq);
                if (sim >= threshold)
                {
                    ulong bucketIndex = *(ulong*)(_base + _bucketIndexesOff + (long)i * 8);
                    sink.Add(new Hit(code, bucketIndex, GuidAt(i), sim));
                }
            }
        }

        private (int lo, int hi) EqualRange(ulong code)
        {
            int lo = LowerBound(code);
            if (lo >= _count || _codes[lo] != code) return (0, 0);
            int hi = lo + 1;
            while (hi < _count && _codes[hi] == code) hi++;
            return (lo, hi);
        }

        private int LowerBound(ulong code)
        {
            int lo = 0, hi = _count;
            while (lo < hi)
            {
                int mid = (int)(((uint)lo + (uint)hi) >> 1);
                if (_codes[mid] < code) lo = mid + 1;
                else hi = mid;
            }
            return lo;
        }

        private unsafe string GuidAt(int i)
        {
            if (_oddGuids != null && _oddGuids.TryGetValue(i, out var odd))
                return odd;
            var span = new ReadOnlySpan<byte>(_base + _guidsOff + (long)i * 32, 32);
            return Convert.ToHexString(span).ToLowerInvariant();
        }

        public void Dispose()
        {
            try { _view.SafeMemoryMappedViewHandle.ReleasePointer(); } catch { }
            try { _view.Dispose(); } catch { }
            try { _mmf.Dispose(); } catch { }
        }
    }
}
