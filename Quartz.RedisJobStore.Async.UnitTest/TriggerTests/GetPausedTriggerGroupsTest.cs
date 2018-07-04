namespace Quartz.RedisJobStore.Async.UnitTest.TriggerTests
{
    #region

    using System.Threading.Tasks;

    using FluentAssertions;

    using NSubstitute;

    using NUnit.Framework;

    using Quartz.RedisJobStore.Async.Enums;

    using StackExchange.Redis;

    #endregion

    [TestFixture]
    public class GetPausedTriggerGroupsTest : FixtureTestBase
    {
        [Test]
        public async Task RedisNoAnyPausedTriggersShouldBeReturnEmpty()
        {
            redis.SetMembersAsync(schema.RedisTriggerGroupStateKey(TriggerRedisState.Paused)).Returns(new RedisValue[0]);

            var result = storage.GetPausedTriggerGroupsAsync();

            (await result).Should().BeEmpty();
        }
        
        [Test]
        public async Task RedisOneAnyPausedTriggersShouldBeReturnOneRecord()
        {
            redis.SetMembersAsync(schema.RedisTriggerGroupStateKey(TriggerRedisState.Paused)).Returns(new RedisValue[]
                                                                                                          {
                                                                                                              "Unit"
                                                                                                          });

            var result = storage.GetPausedTriggerGroupsAsync();

            (await result).Should().HaveCount(1);
        }
        
        [Test]
        public async Task RedisOneAnyPausedTriggersShouldBeEqualRedisStoreData()
        {
            redis.SetMembersAsync(schema.RedisTriggerGroupStateKey(TriggerRedisState.Paused)).Returns(new RedisValue[]
                                                                                                          {
                                                                                                              "Unit"
                                                                                                          });

            var result = storage.GetPausedTriggerGroupsAsync();

            (await result).Should().Contain("Unit");
        }
    }
}