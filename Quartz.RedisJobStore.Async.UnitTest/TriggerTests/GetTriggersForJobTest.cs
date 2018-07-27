using System.Threading.Tasks;
using FluentAssertions;
using NSubstitute;
using NUnit.Framework;
using StackExchange.Redis;

namespace Quartz.RedisJobStore.Async.UnitTest.TriggerTests
{
    [TestFixture]
    public class GetTriggersForJobTest : FixtureTestBase
    {
        [Test]
        public async Task GetTriggerForJobWillNotBeThrow()
        {
            var jobKey = new JobKey("Test", "Unit");
            var result = storage.GetTriggersForJobAsync(jobKey);

            (await result).Should().BeEmpty();
        }
        
        [Test]
        public async Task GetTriggerForJobWillBeNoAnyTriggers()
        {
            var jobKey = new JobKey("Test", "Unit");
            redis.SetMembersAsync(schema.RedisTriggerJobKey(jobKey)).Returns(new RedisValue[]
            {
                "Test:Trigger"
            });
            
            var result = storage.GetTriggersForJobAsync(jobKey);

            (await result).Should().BeEmpty();
        }
    }
}