
using Agent.Modules.Agneta;

public static class BackgrounfServiceManager
{
    public static Dictionary<string ,Action> RoutineMethods = new Dictionary<string, Action>();

    public static void RegisterRoutineMethod(string _name, Action _func)
    {
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
                await AgnetaHandler.Log(2, "")
            }
        }
    }
}