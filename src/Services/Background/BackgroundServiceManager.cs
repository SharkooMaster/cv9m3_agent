
using System.Threading.Tasks;
using Agent.Modules.Agneta;

public static class BackgrounfServiceManager
{
    public static Dictionary<string ,Action> RoutineMethods = new Dictionary<string, Action>();

    public static async Task RegisterRoutineMethod(string _name, Action _func)
    {
        if(!RoutineMethods.TryAdd(_name, _func))
        {
            await AgnetaHandler.Log(2, $"Failed to register routine: {_name}");
        }
    }

    public static async void RunRoutineMethods()
    {
        string[] RoutineMethodsKeys = RoutineMethods.Keys.ToArray();
        for (int i = 0; i < RoutineMethods.Count; i++)
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

}
