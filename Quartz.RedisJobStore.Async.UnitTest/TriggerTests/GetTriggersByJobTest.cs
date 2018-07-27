namespace Quartz.RedisJobStore.Async.UnitTest.TriggerTests
{
    #region

    using System;
    using System.Threading.Tasks;

    using FluentAssertions;

    using NSubstitute;

    using NUnit.Framework;

    using Quartz.RedisJobStore.Async.Enums;

    using StackExchange.Redis;

    #endregion

    [TestFixture]
    public class GetTriggersByJobTest : FixtureTestBase
    {
        [Test]
        public async Task RedisReturnEmptyListShouldBeReturnEmptyList()
        {
            var key = new JobKey("Unit", "Test");
            redis.SetMembersAsync(schema.RedisTriggerJobKey(key)).Returns(new RedisValue[0]);

            var result = storage.GetTriggersByJobAsync(key);

            (await result).Should().HaveCount(0);
        }

        [Test]
        public async Task RedisReturnOneRecordShouldBeReturnOneRecord()
        {
            var key = InitializeJobKey();
            InitializeTriggerKey(key);

            var result = storage.GetTriggersByJobAsync(key);

            (await result).Should().HaveCount(1);
        }

        [Test]
        public async Task RedisReturnOneRecordTriggerNameShouldBeEqual()
        {
            var key = InitializeJobKey();
            var triggerKey = InitializeTriggerKey(key);
            

            var result = storage.GetTriggersByJobAsync(key);

            (await result).Should().HaveCount(1);
            (await result).Should().Contain(x => schema.RedisTriggerKey(x.Key) == schema.RedisTriggerKey(triggerKey));
        }

        private JobKey InitializeJobKey()
        {
            var key = new JobKey("Unit", "Test");
            redis.SetMembersAsync(schema.RedisTriggerJobKey(key))
                 .Returns(
                      new RedisValue[]
                          {
                              "Unit:Test1"
                          });
            return key;
        }

        private TriggerKey InitializeTriggerKey(JobKey job)
        {
            var triggerKey = new TriggerKey("Test1", "Unit");
            redis.HashGetAllAsync(schema.RedisTriggerKey(triggerKey)).Returns(SetRedisTriggerData(TriggerStoreKey.TriggerTypeSimple, job));
            return triggerKey;
        }

        private HashEntry[] SetRedisTriggerData(string triggerType, JobKey job)
        {
            return new[]
                       {
                           new HashEntry(TriggerStoreKey.JobHash, schema.JobStoreKey(job)),
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
                           new HashEntry(TriggerStoreKey.CronExpression, "* * * * * ? *")
                       };
        }
    }
}