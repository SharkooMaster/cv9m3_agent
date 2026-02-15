using System.Diagnostics;

namespace Agent.Utils;

/// <summary>
/// Dynamically adjusts concurrency limits based on current CPU and memory usage.
/// Prevents resource overuse by scaling down when system is under load.
/// </summary>
public static class DynamicResourceManager
{
    private static readonly object _lock = new object();
    private static DateTime _lastCheck = DateTime.MinValue;
    private static double _lastCpuLoad = 0.0;
    private static double _lastMemoryUsage = 0.0;
    private static readonly TimeSpan _checkInterval = TimeSpan.FromSeconds(2);

    private const double CPU_HIGH_THRESHOLD = 0.80;
    private const double CPU_MEDIUM_THRESHOLD = 0.60;
    private const double MEMORY_HIGH_THRESHOLD = 0.85;
    private const double MEMORY_MEDIUM_THRESHOLD = 0.70;

    public static int GetOptimalConcurrency(int minConcurrency, int maxConcurrency, int baseConcurrency)
    {
        lock (_lock)
        {
            if (DateTime.UtcNow - _lastCheck > _checkInterval)
            {
                UpdateMetrics();
            }

            double cpuFactor = GetCpuAdjustmentFactor();
            double memoryFactor = GetMemoryAdjustmentFactor();
            double adjustmentFactor = Math.Min(cpuFactor, memoryFactor);

            int optimalConcurrency = (int)(baseConcurrency * adjustmentFactor);
            optimalConcurrency = Math.Max(minConcurrency, Math.Min(maxConcurrency, optimalConcurrency));

            return optimalConcurrency;
        }
    }

    public static int GetOptimalParallelism(int baseParallelism)
    {
        int minParallelism = Math.Max(1, baseParallelism / 4);
        int maxParallelism = baseParallelism;
        return GetOptimalConcurrency(minParallelism, maxParallelism, baseParallelism);
    }

    private static void UpdateMetrics()
    {
        try
        {
            _lastCpuLoad = GetCurrentCpuLoad();
            _lastMemoryUsage = GetCurrentMemoryUsage();
            _lastCheck = DateTime.UtcNow;
        }
        catch { }
    }

    private static double GetCpuAdjustmentFactor()
    {
        int processorCount = Environment.ProcessorCount;
        double normalizedLoad = _lastCpuLoad / processorCount;

        if (normalizedLoad >= CPU_HIGH_THRESHOLD) return 0.30;
        else if (normalizedLoad >= CPU_MEDIUM_THRESHOLD) return 0.50;
        else if (normalizedLoad >= 0.40) return 0.75;
        else return 1.0;
    }

    private static double GetMemoryAdjustmentFactor()
    {
        if (_lastMemoryUsage >= MEMORY_HIGH_THRESHOLD) return 0.30;
        else if (_lastMemoryUsage >= MEMORY_MEDIUM_THRESHOLD) return 0.50;
        else if (_lastMemoryUsage >= 0.50) return 0.75;
        else return 1.0;
    }

    private static double GetCurrentCpuLoad()
    {
        try
        {
            if (File.Exists("/proc/loadavg"))
            {
                string[] parts = File.ReadAllText("/proc/loadavg").Split(' ', StringSplitOptions.RemoveEmptyEntries);
                return double.Parse(parts[0]);
            }
        }
        catch { }

        try
        {
            var process = Process.GetCurrentProcess();
            var cpuTime = process.TotalProcessorTime.TotalMilliseconds;
            var elapsed = (DateTime.UtcNow - process.StartTime).TotalMilliseconds;
            return (cpuTime / elapsed) * Environment.ProcessorCount;
        }
        catch { }

        return 0.0;
    }

    private static double GetCurrentMemoryUsage()
    {
        try
        {
            if (File.Exists("/proc/meminfo"))
            {
                double totalMemory = 0;
                double freeMemory = 0;

                var lines = File.ReadAllLines("/proc/meminfo");
                foreach (var line in lines)
                {
                    if (line.StartsWith("MemTotal:"))
                    {
                        totalMemory = ParseMemValue(line);
                    }
                    else if (line.StartsWith("MemAvailable:"))
                    {
                        freeMemory = ParseMemValue(line);
                        break;
                    }
                }

                if (totalMemory > 0)
                {
                    double usedMemory = totalMemory - freeMemory;
                    return usedMemory / totalMemory;
                }
            }
        }
        catch { }

        try
        {
            var gcInfo = GC.GetGCMemoryInfo();
            var totalMemory = gcInfo.TotalAvailableMemoryBytes;
            var heapMemory = gcInfo.HeapSizeBytes;
            return (double)heapMemory / totalMemory;
        }
        catch { }

        return 0.0;
    }

    private static double ParseMemValue(string line)
    {
        var parts = line.Split(new[] { ' ', '\t' }, StringSplitOptions.RemoveEmptyEntries);
        return double.Parse(parts[1]) / 1024;
    }
}

