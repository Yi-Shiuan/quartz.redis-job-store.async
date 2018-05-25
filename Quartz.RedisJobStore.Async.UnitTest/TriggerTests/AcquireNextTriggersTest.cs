namespace Quartz.RedisJobStore.Async.UnitTest.TriggerTests
{
    using System;
    using System.Linq;
    using System.Threading.Tasks;

    using FluentAssertions;

    using NSubstitute;

    using NUnit.Framework;

    using StackExchange.Redis;

    using Quartz.RedisJobStore.Async.Enums;

    [TestFixture]
    public class AcquireNextTriggersTest : FixtureTestBase
    {
        [Test]
        public async Task AcquireNextTriggerNotTriggerAcquire()
        {
            var noLaterThan = DateTimeOffset.UtcNow;
            var score = storage.ToUnixTimeMilliseconds(noLaterThan.Add(TimeSpan.Zero));
            redis.SortedSetRangeByScoreWithScoresAsync(schema.TriggerStateKey(TriggerState.Waiting), 0, score, Exclude.None, Order.Ascending, 0, 1)
                 .Returns(new SortedSetEntry[0]);

            var triggers = await storage.AcquireNextTriggersAsync(noLaterThan, 1, TimeSpan.Zero);

            triggers.Should().BeEmpty();
        }

        [Test]
        public async Task AcquireNextTriggerHaveOneTriggerAcquire()
        {
            var noLaterThan = DateTimeOffset.UtcNow;
            var score = storage.ToUnixTimeMilliseconds(noLaterThan.Add(TimeSpan.Zero));
            redis.SortedSetRangeByScoreWithScoresAsync(schema.TriggerStateKey(TriggerState.Waiting), 0, score, Exclude.None, Order.Ascending, 0, 1)
                 .Returns(new[]
                              {
                                  new SortedSetEntry(schema.TriggerHashKey(new TriggerKey("case1", "UnitTest")), score)
                              });

            var triggers = await storage.AcquireNextTriggersAsync(noLaterThan, 1, TimeSpan.Zero);

            triggers.Should().HaveCount(1);
        }

        [Test]
        public async Task AcquireNextTriggerHaveTwoTriggerAcquire()
        {
            var noLaterThan = DateTimeOffset.UtcNow;
            var score = storage.ToUnixTimeMilliseconds(noLaterThan.Add(TimeSpan.Zero));
            redis.SortedSetRangeByScoreWithScoresAsync(schema.TriggerStateKey(TriggerState.Waiting), 0, score, Exclude.None, Order.Ascending, 0, 1)
                 .Returns(new[]
                              {
                                  new SortedSetEntry(schema.TriggerHashKey(new TriggerKey("case1", "UnitTest")), score),
                                  new SortedSetEntry(schema.TriggerHashKey(new TriggerKey("case2", "UnitTest")), score)
                              });

            var triggers = await storage.AcquireNextTriggersAsync(noLaterThan, 1, TimeSpan.Zero);

            triggers.Should().HaveCount(2);
        }

        [Test]
        public async Task AcquireNextTriggerHaveOneTriggerAcquireShouldBeStoreToRedis()
        {
            var noLaterThan = DateTimeOffset.UtcNow;
            var score = storage.ToUnixTimeMilliseconds(noLaterThan.Add(TimeSpan.Zero));
            var triggerHashKey = schema.TriggerHashKey(new TriggerKey("case1", "UnitTest"));
            redis.SortedSetRangeByScoreWithScoresAsync(schema.TriggerStateKey(TriggerState.Waiting), 0, score, Exclude.None, Order.Ascending, 0, 1)
                 .Returns(new[]
                              {
                                  new SortedSetEntry(triggerHashKey, score)
                              });

            var triggers = await storage.AcquireNextTriggersAsync(noLaterThan, 1, TimeSpan.Zero);

            redis.Received().SortedSetAddAsync(schema.TriggerStateKey(TriggerState.Acquired), triggerHashKey, score);
            triggers.Should().HaveCount(1);
        }

        [Test]
        public async Task AcquireNextTriggerHaveOneTriggerAcquireShouldBeGetCorrectData()
        {
            var noLaterThan = DateTimeOffset.UtcNow;
            var score = storage.ToUnixTimeMilliseconds(noLaterThan.Add(TimeSpan.Zero));
            var triggerKey = new TriggerKey("case1", "UT");
            var triggerHashKey = schema.TriggerHashKey(triggerKey);
            redis.SortedSetRangeByScoreWithScoresAsync(schema.TriggerStateKey(TriggerState.Waiting), 0, score, Exclude.None, Order.Ascending, 0, 1)
                 .Returns(new[]
                              {
                                  new SortedSetEntry(triggerHashKey, score)
                              });
            redis.HashGetAllAsync(triggerHashKey).Returns(RestoreTrigger(schema.JobHashKey(new JobKey("case1", "UT"))));

            var triggers = await storage.AcquireNextTriggersAsync(noLaterThan, 1, TimeSpan.Zero);

            redis.Received().SortedSetAddAsync(schema.TriggerStateKey(TriggerState.Acquired), triggerHashKey, score);
            triggers.First().Key.Should().Be(triggerKey);
        }

        private HashEntry[] RestoreTrigger(string job)
        {
            return new[]
                       {
                           new HashEntry(TriggerStoreKey.JobHash, job),
                           new HashEntry(TriggerStoreKey.Description, string.Empty),
                           new HashEntry(TriggerStoreKey.NextFireTime, "1522749600000"),
                           new HashEntry(TriggerStoreKey.PrevFireTime, string.Empty),
                           new HashEntry(TriggerStoreKey.Priority, "5"),
                           new HashEntry(TriggerStoreKey.StartTime, "1522749600000"),
                           new HashEntry(TriggerStoreKey.EndTime, string.Empty),
                           new HashEntry(TriggerStoreKey.FinalFireTime, string.Empty),
                           new HashEntry(TriggerStoreKey.FireInstanceId, string.Empty),
                           new HashEntry(TriggerStoreKey.MisfireInstruction, "-1"),
                           new HashEntry(TriggerStoreKey.CalendarName, string.Empty),
                           new HashEntry(TriggerStoreKey.TriggerType, "CRON"),
                           new HashEntry(TriggerStoreKey.TimeZoneId, TimeZoneInfo.Utc.Id),
                           new HashEntry(TriggerStoreKey.CronExpression, "* 0 0 ? * * *"),
                       };
        }

        private HashEntry[] RestoreJob()
        {
            return new[]
                       {
                           new HashEntry(JobStoreKey.JobClass, typeof(TestJob).AssemblyQualifiedName),
                           new HashEntry(JobStoreKey.Description, "this for unit test"),
                           new HashEntry(JobStoreKey.RequestRecovery, true),
                           new HashEntry(JobStoreKey.IsDurable, false)
                       };
        }
    }
}