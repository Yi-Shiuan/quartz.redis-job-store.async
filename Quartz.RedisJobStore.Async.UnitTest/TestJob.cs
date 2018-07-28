using System;
using System.Threading.Tasks;

namespace Quartz.RedisJobStore.Async.UnitTest
{
    public class TestJob : IJob
    {
        public Task Execute(IJobExecutionContext context)
        {
            throw new NotImplementedException();
        }
    }
}