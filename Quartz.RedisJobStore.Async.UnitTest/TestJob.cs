namespace Quartz.RedisJobStore.Async.UnitTest
{
    using System.Threading.Tasks;

    public class TestJob : IJob
    {
        public Task Execute(IJobExecutionContext context)
        {
            throw new System.NotImplementedException();
        }
    }
}