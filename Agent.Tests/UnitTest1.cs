using Agent.Misc;

namespace Agent.Tests;

[Trait("Category", "Unit")]
public class UnitTest1
{
    [Fact]
    public void Test1()
    {
        Assert.Equal(2, CI_Pipeline_Test.Int_Ret_Test(1));
    }
}