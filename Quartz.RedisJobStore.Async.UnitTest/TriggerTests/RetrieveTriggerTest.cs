namespace Quartz.RedisJobStore.Async.UnitTest.TriggerTests
{
    #region

    using System;
    using System.Linq;
    using System.Threading.Tasks;

    using FluentAssertions;

    using NSubstitute;

    using NUnit.Framework;

    using Quartz.Impl.Triggers;
    using Quartz.RedisJobStore.Async.Enums;

    using StackExchange.Redis;

    #endregion

    [TestFixture]
    public class RetrieveTriggerTest : FixtureTestBase
    {
        [Test]
        public async Task RedisReturnTriggerTypeNotEmptyShouldNotBeReturnNull()
        {
            var key = new TriggerKey("Unit", "Test");
            redis.HashGetAllAsync(schema.RedisTriggerKey(key)).Returns(SetRedisTriggerData("Simple"));
            var result = storage.RetrieveTriggerAsync(key);

            (await result).Should().NotBeNull();
        }
        
        [Test]
        public async Task RedisReturnTriggerTypeIsEmptyShouldBeReturnNull()
        {
            var key = new TriggerKey("Unit", "Test");
            redis.HashGetAllAsync(schema.RedisTriggerKey(key)).Returns(SetRedisTriggerData(string.Empty));
            var result = storage.RetrieveTriggerAsync(key);

            (await result).Should().BeNull();
        }

        [Test]
        public async Task RedisReturnTriggerTypeIsCronShouldBeReturnCronType()
        {
            var key = new TriggerKey("Unit", "Test");
            redis.HashGetAllAsync(schema.RedisTriggerKey(key)).Returns(SetRedisTriggerData(TriggerStoreKey.TriggerTypeCron));
            var result = storage.RetrieveTriggerAsync(key);

            (await result).GetType().Should().Be(typeof(CronTriggerImpl));
            ((CronTriggerImpl)await result).CronExpressionString.Should().NotBeNullOrEmpty();
            ((CronTriggerImpl)await result).TimeZone.Should().NotBeNull();
        }
        
        [Test]
        public async Task RedisReturnTriggerTypeIsSimpleShouldBeReturnSimpleType()
        {
            var key = new TriggerKey("Unit", "Test");
            redis.HashGetAllAsync(schema.RedisTriggerKey(key)).Returns(SetRedisTriggerData(TriggerStoreKey.TriggerTypeSimple));
            var result = storage.RetrieveTriggerAsync(key);

            (await result).GetType().Should().Be(typeof(SimpleTriggerImpl));
            ((SimpleTriggerImpl)await result).RepeatCount.Should().BeGreaterOrEqualTo(0);
            ((SimpleTriggerImpl)await result).TimesTriggered.Should().BeGreaterOrEqualTo(0);
            ((SimpleTriggerImpl)await result).RepeatInterval.Should().BePositive();
        }
        
        [Test]
        public async Task RedisReturnTriggerShouldBeReturnSimpleType()
        {
            var key = new TriggerKey("Unit", "Test");
            redis.HashGetAllAsync(schema.RedisTriggerKey(key)).Returns(SetRedisTriggerData(TriggerStoreKey.TriggerTypeSimple));
            var result = storage.RetrieveTriggerAsync(key);

            (await result).GetType().Should().Be(typeof(SimpleTriggerImpl));
            ((SimpleTriggerImpl)await result).RepeatCount.Should().BeGreaterOrEqualTo(0);
            ((SimpleTriggerImpl)await result).TimesTriggered.Should().BeGreaterOrEqualTo(0);
            ((SimpleTriggerImpl)await result).RepeatInterval.Should().BePositive();
        }

        private HashEntry[] SetRedisTriggerData(string triggerType)
        {
            return new[]
                          {
                              new HashEntry(TriggerStoreKey.JobHash, "UT:job1"),
                              new HashEntry(TriggerStoreKey.Description, string.Empty),
                              new HashEntry(TriggerStoreKey.NextFireTime, "1522749600000"),
                              new HashEntry(TriggerStoreKey.PrevFireTime, string.Empty),
                              new HashEntry(TriggerStoreKey.Priority, "5"),
                              new HashEntry(TriggerStoreKey.StartTime, "1522749600000"),
                              new HashEntry(TriggerStoreKey.EndTime, string.Empty),
                              new HashEntry(TriggerStoreKey.FinalFireTime, string.Empty),
                              new HashEntry(TriggerStoreKey.FireInstanceId, string.Empty),
                              new HashEntry(TriggerStoreKey.MisfireInstruction, "0"),
                              new HashEntry(TriggerStoreKey.CalendarName, string.Empty),
                              new HashEntry(TriggerStoreKey.TriggerType, triggerType),
                              new HashEntry(TriggerStoreKey.RepeatCount, "0"),
                              new HashEntry(TriggerStoreKey.RepeatInterval, "00:00:01"),
                              new HashEntry(TriggerStoreKey.TimesTriggered, "0"),
                              new HashEntry(TriggerStoreKey.TimeZoneId, TimeZoneInfo.Local.Id),
                              new HashEntry(TriggerStoreKey.CronExpression, "* * * * * ? *"), 
                          };
        }
    }
}