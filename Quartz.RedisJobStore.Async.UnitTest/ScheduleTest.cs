namespace Quartz.RedisJobStore.Async.UnitTest
{
    using System.Threading.Tasks;

    using NUnit.Framework;

    using Quartz.Impl;

    [TestFixture]
    public class ScheduleTest
    {
        [Test]
        public async Task InitializationTest()
        {
            var scheduler = await StdSchedulerFactory.GetDefaultScheduler();
            await scheduler.Start();
            var a = 1;
        }
    }
}