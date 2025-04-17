
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Agent.Modules.Agneta;

public static class BackgrounfServiceManager
{
    public static ConcurrentDictionary<string ,Action> RoutineMethods = new ConcurrentDictionary<string, Action>();
    public static ConcurrentDictionary<string ,Action> FireMethods = new ConcurrentDictionary<string, Action>();

    public static async Task RegisterRoutineMethod(string _name, Action _func)
    {
        if(!RoutineMethods.TryAdd(_name, _func))
        {
            await AgnetaHandler.Log(2, $"Failed to register routine: {_name}");
        }
    }

    public static async Task RegisterFireMethod(string _name, Action _func)
    {
        if(!FireMethods.TryAdd(_name, _func))
        {
            // await AgnetaHandler.Log(2, $"Failed to register routine: {_name}");
            Console.WriteLine($"Failed to register routine: {_name}");
        }
    }

    public static async Task RunRoutineMethods()
    {
        string[] RoutineMethodsKeys = RoutineMethods.Keys.ToArray();
        for (int i = 0; i < RoutineMethodsKeys.Length; i++)
        {
            try
            {
                RoutineMethods[RoutineMethodsKeys[i]]();
            }
            catch (System.Exception)
            {
                await AgnetaHandler.Log(2, $"Failed to run routine {RoutineMethodsKeys[i]}");
            }
        }
    }

    public static async Task RunFireMethods()
    {
        string[] FireMethodsKeys = FireMethods.Keys.ToArray();
        ParallelOptions options = new ParallelOptions() { MaxDegreeOfParallelism = 2 };
        await Parallel.ForAsync(0, FireMethodsKeys.Length, options, async (i, ct) =>
        {
            try
            {
                FireMethods[FireMethodsKeys[i]]();
                if(FireMethods.TryRemove(FireMethodsKeys[i], out _))
                {
                }
            }
            catch (System.Exception)
            {
                // await AgnetaHandler.Log(2, $"Failed to run FireRoutine {FireMethodsKeys[i]}");
                Console.WriteLine($"Failed to run FireRoutine {FireMethodsKeys[i]}");
            }
        });
    }

}
