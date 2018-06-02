using System.Threading.Tasks;
using FluentAssertions;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using NUnit.Framework;
using StackExchange.Redis;

namespace Quartz.RedisJobStore.Async.UnitTest.JobTests
{
    [TestFixture]
    public class StoreJobTest : FixtureTestBase
    {
        [Test]
        public async Task StoreJobShouldBeStoreToJobGroup()
        {
            var job = JobBuilder.Create<TestJob>().Build();
            var result = storage.StoreJobAsync(job, true);

            await result;

            redis.Received().SetAdd(schema.RedisJobGroupKey(job.Key), schema.JobStoreKey(job.Key),
                CommandFlags.FireAndForget);
        }
        
        [Test]
        public async Task StoreJobShouldBeStoreToJobsKey()
        {
            var job = JobBuilder.Create<TestJob>().Build();
            var result = storage.StoreJobAsync(job, true);

            await result;

            redis.Received().SetAdd(schema.RedisJobKey(), schema.JobStoreKey(job.Key),
                CommandFlags.FireAndForget);
        }
        
        [Test]
        public async Task StoreJobShouldBeStoreToJobGroups()
        {
            var job = JobBuilder.Create<TestJob>().Build();
            var result = storage.StoreJobAsync(job, true);

            await result;

            redis.Received().SetAdd(schema.RedisJobGroupKey(), schema.JobGroupStoreKey(job.Key),
                CommandFlags.FireAndForget);
        }
        
        [Test]
        public async Task StoreJobShouldBeStoreToJobDataMap()
        {
            var job = JobBuilder.Create<TestJob>().Build();
            var result = storage.StoreJobAsync(job, true);

            await result;

            redis.Received().HashSet(schema.RedisJobDataMap(job.Key), Arg.Any<HashEntry[]>(),
                CommandFlags.FireAndForget);
        }
        
        [Test]
        public async Task StoreJobShouldBeStoreToJobKey()
        {
            var job = JobBuilder.Create<TestJob>().Build();
            var result = storage.StoreJobAsync(job, true);

            await result;

            redis.Received().HashSet(schema.RedisJobKey(job.Key), Arg.Any<HashEntry[]>(),
                CommandFlags.FireAndForget);
        }
        
        [Test]
        public async Task StoreDuplicateJobShouldBeStoreToJobKey()
        {
            var job = JobBuilder.Create<TestJob>().Build();
            redis.KeyExistsAsync(schema.RedisJobKey(job.Key)).Returns(true);
            
            var result = storage.StoreJobAsync(job, false);

            Assert.ThrowsAsync<ObjectAlreadyExistsException>(async () => await result);
        }
    }
}