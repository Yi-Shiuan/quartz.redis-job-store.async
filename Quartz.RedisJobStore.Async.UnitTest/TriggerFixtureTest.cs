namespace Quartz.RedisJobStore.Async.UnitTest
{
    #region

    using System;
    using System.Threading.Tasks;

    using NSubstitute;

    using NUnit.Framework;

    using Quartz.Impl.Triggers;
    using Quartz.RedisJobStore.Async.Enums;

    using StackExchange.Redis;

    #endregion

    [TestFixture]
    public class TriggerFixtureTest : FixtureTestBase
    {
        private readonly string description = "UT trigger description";

        private readonly string group = "UT";

        private readonly string name = "case1";

        [Test]
        [TestCase(null, false)]
        [TestCase(9d, true)]
        public void PauseTriggerShouldBeNotThrow(double? complate, bool existed)
        {
            var trigger = new TriggerKey(name, group);

            redis.KeyExistsAsync(schema.TriggerHashKey(trigger)).Returns(existed);
            redis.SortedSetScoreAsync(schema.TriggerStateKey(TriggerState.Completed), schema.TriggerHashKey(trigger)).Returns(complate);

            Assert.DoesNotThrowAsync(async () => await storage.PauseTriggerAsync(trigger));

            redis.DidNotReceive()
                 .SortedSetAddAsync(schema.TriggerStateKey(TriggerState.Paused), schema.TriggerHashKey(trigger), Arg.Any<double>());
        }

        [Test]
        [TestCase(null, TriggerState.Paused)]
        [TestCase(9, TriggerState.PausedBlocked)]
        public void PauseTriggerShouldBeSuccessfully(double? blocked, TriggerState state)
        {
            var trigger = new TriggerKey(name, group);

            redis.KeyExistsAsync(schema.TriggerHashKey(trigger)).Returns(true);
            redis.SortedSetScoreAsync(schema.TriggerStateKey(TriggerState.Completed), schema.TriggerHashKey(trigger)).Returns((double?)null);
            redis.SortedSetScoreAsync(schema.TriggerStateKey(TriggerState.Blocked), schema.TriggerHashKey(trigger)).Returns(blocked);
            redis.HashGetAsync(schema.TriggerHashKey(trigger), TriggerStoreKey.NextFireTime).Returns(999999);

            Assert.DoesNotThrowAsync(async () => await storage.PauseTriggerAsync(trigger));

            redis.Received().SortedSetAddAsync(schema.TriggerStateKey(state), schema.TriggerHashKey(trigger), Arg.Any<double>());
        }

        [Test]
        public void ResumeTriggerButTriggerNotPauseShouldBeNotThrow()
        {
            var trigger = new TriggerKey(name, group);

            redis.SortedSetScoreAsync(schema.TriggerStateKey(TriggerState.Paused), schema.TriggerHashKey(trigger)).Returns((double?)null);
            redis.SortedSetScoreAsync(schema.TriggerStateKey(TriggerState.PausedBlocked), schema.TriggerHashKey(trigger)).Returns((double?)null);

            Assert.DoesNotThrowAsync(async () => await storage.ResumeTriggerAsync(trigger));

            redis.DidNotReceive()
                 .SortedSetAddAsync(schema.TriggerStateKey(TriggerState.Waiting), schema.TriggerHashKey(trigger), Arg.Any<double>());
            redis.DidNotReceive()
                 .SortedSetAddAsync(schema.TriggerStateKey(TriggerState.Blocked), schema.TriggerHashKey(trigger), Arg.Any<double>());
        }

        [Test]
        [TestCase(null, 0, true, TriggerState.Blocked)]
        [TestCase(0, null, false, TriggerState.Waiting)]
        public void ResumeTriggerShouldBeSuccessfully(double? paused, double? pauseBlocked, bool isBlockedJob, TriggerState state)
        {
            var trigger = new TriggerKey(name, group);

            redis.SortedSetScoreAsync(schema.TriggerStateKey(TriggerState.Paused), schema.TriggerHashKey(trigger)).Returns(paused);
            redis.SortedSetScoreAsync(schema.TriggerStateKey(TriggerState.PausedBlocked), schema.TriggerHashKey(trigger)).Returns(pauseBlocked);
            redis.HashGetAsync(schema.TriggerHashKey(trigger), TriggerStoreKey.NextFireTime).Returns(999999);
            redis.SetContainsAsync(schema.BlockedJobsSet(), schema.JobHashKey(new JobKey("Job", "UT"))).Returns(isBlockedJob);

            redis.HashGetAllAsync(schema.TriggerHashKey(trigger))
                 .Returns(
                      new[]
                          {
                              new HashEntry(TriggerStoreKey.JobHash, schema.JobHashKey(new JobKey("Job", "UT"))),
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
                              new HashEntry(TriggerStoreKey.TriggerType, "Simple"),
                              new HashEntry(TriggerStoreKey.RepeatCount, "0"),
                              new HashEntry(TriggerStoreKey.RepeatInterval, "00:00:00"),
                              new HashEntry(TriggerStoreKey.TimesTriggered, "0")
                          });

            Assert.DoesNotThrowAsync(async () => await storage.ResumeTriggerAsync(trigger));
        }

        [Test]
        public void StoreNotImplementedTriggerShouldBeThrow()
        {
            Assert.ThrowsAsync<NotImplementedException>(async () => await storage.StoreTriggerAsync(Substitute.For<ITrigger>(), true));
        }

        [Test]
        public void StoreSimpleTriggerAndTriggerExistShouldBeSuccessfully()
        {
            var trigger = TriggerBuilder.Create()
                                        .StartNow()
                                        .WithIdentity(name, group)
                                        .WithDescription(description)
                                        .ForJob("job", group)
                                        .WithSimpleSchedule(x => x.WithIntervalInSeconds(10).RepeatForever())
                                        .Build();
            ComputeFirstFireTime(trigger);
            redis.KeyExistsAsync(schema.TriggerHashKey(trigger.Key)).Returns(true);

            redis.When(x => x.HashSetAsync(schema.TriggerHashKey(trigger.Key), Arg.Any<HashEntry[]>()))
                 .Do(
                      info =>
                          {
                              var hashSet = info.Arg<HashEntry[]>().ToStringDictionary();
                              Assert.AreEqual(15, info.Arg<HashEntry[]>().Length);
                              Assert.NotNull(hashSet[TriggerStoreKey.JobHash]);
                              Assert.NotNull(hashSet[TriggerStoreKey.Description]);
                              Assert.NotNull(hashSet[TriggerStoreKey.NextFireTime]);
                              Assert.NotNull(hashSet[TriggerStoreKey.PrevFireTime]);
                              Assert.NotNull(hashSet[TriggerStoreKey.Priority]);
                              Assert.NotNull(hashSet[TriggerStoreKey.StartTime]);
                              Assert.NotNull(hashSet[TriggerStoreKey.EndTime]);
                              Assert.NotNull(hashSet[TriggerStoreKey.FinalFireTime]);
                              Assert.NotNull(hashSet[TriggerStoreKey.FireInstanceId]);
                              Assert.NotNull(hashSet[TriggerStoreKey.MisfireInstruction]);
                              Assert.NotNull(hashSet[TriggerStoreKey.CalendarName]);
                              Assert.NotNull(hashSet[TriggerStoreKey.TriggerType]);
                              Assert.NotNull(hashSet[TriggerStoreKey.RepeatCount]);
                              Assert.NotNull(hashSet[TriggerStoreKey.RepeatInterval]);
                              Assert.NotNull(hashSet[TriggerStoreKey.TimesTriggered]);
                          });

            Assert.DoesNotThrowAsync(async () => await storage.StoreTriggerAsync(trigger, true));
            redis.Received(1).SetAdd(schema.TriggersKey(), schema.TriggerHashKey(trigger.Key), CommandFlags.FireAndForget);
            redis.Received().SortedSetRemoveAsync(schema.TriggerStateKey(Arg.Any<TriggerState>()), schema.TriggerHashKey(trigger.Key));
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public void StoreSimpleTriggerAndTriggerNotExistShouldBeSuccessfully(bool replaceExisting)
        {
            var trigger = TriggerBuilder.Create()
                                        .StartNow()
                                        .WithIdentity(name, group)
                                        .WithDescription(description)
                                        .ForJob("job", group)
                                        .WithSimpleSchedule(x => x.WithIntervalInSeconds(10).RepeatForever())
                                        .Build();
            ComputeFirstFireTime(trigger);
            redis.KeyExistsAsync(schema.TriggerHashKey(trigger.Key)).Returns(false);

            redis.When(x => x.HashSetAsync(schema.TriggerHashKey(trigger.Key), Arg.Any<HashEntry[]>()))
                 .Do(
                      info =>
                          {
                              var hashSet = info.Arg<HashEntry[]>().ToStringDictionary();
                              Assert.AreEqual(15, info.Arg<HashEntry[]>().Length);
                              Assert.NotNull(hashSet[TriggerStoreKey.JobHash]);
                              Assert.NotNull(hashSet[TriggerStoreKey.Description]);
                              Assert.NotNull(hashSet[TriggerStoreKey.NextFireTime]);
                              Assert.NotNull(hashSet[TriggerStoreKey.PrevFireTime]);
                              Assert.NotNull(hashSet[TriggerStoreKey.Priority]);
                              Assert.NotNull(hashSet[TriggerStoreKey.StartTime]);
                              Assert.NotNull(hashSet[TriggerStoreKey.EndTime]);
                              Assert.NotNull(hashSet[TriggerStoreKey.FinalFireTime]);
                              Assert.NotNull(hashSet[TriggerStoreKey.FireInstanceId]);
                              Assert.NotNull(hashSet[TriggerStoreKey.MisfireInstruction]);
                              Assert.NotNull(hashSet[TriggerStoreKey.CalendarName]);
                              Assert.NotNull(hashSet[TriggerStoreKey.TriggerType]);
                              Assert.NotNull(hashSet[TriggerStoreKey.RepeatCount]);
                              Assert.NotNull(hashSet[TriggerStoreKey.RepeatInterval]);
                              Assert.NotNull(hashSet[TriggerStoreKey.TimesTriggered]);
                          });

            Assert.DoesNotThrowAsync(async () => await storage.StoreTriggerAsync(trigger, replaceExisting));
            redis.Received(1).SetAdd(schema.TriggersKey(), schema.TriggerHashKey(trigger.Key), CommandFlags.FireAndForget);
            redis.Received().SortedSetRemoveAsync(schema.TriggerStateKey(Arg.Any<TriggerState>()), schema.TriggerHashKey(trigger.Key));
        }

        [Test]
        public void StoreTriggerButTriggerIsExistShouldBeThrow()
        {
            var trigger = TriggerBuilder.Create()
                                        .WithIdentity(name, group)
                                        .WithDescription(description)
                                        .ForJob("job", group)
                                        .WithSimpleSchedule(x => x.WithIntervalInSeconds(10).RepeatForever())
                                        .Build();

            redis.KeyExistsAsync(schema.TriggerHashKey(trigger.Key)).Returns(true);

            Assert.ThrowsAsync<ObjectAlreadyExistsException>(async () => await storage.StoreTriggerAsync(trigger, false));
        }

        [Test]
        public async Task RemoveTriggerButTriggerIsNotExistShouldNotThrow()
        {
            var trigger = new TriggerKey(name, @group);

            redis.KeyExistsAsync(schema.TriggerHashKey(trigger)).Returns(false);

            Assert.False(await storage.RemoveTriggerAsync(trigger, false));
        }

        [Test]
        [TestCase(true, 1, "", 0)]
        [TestCase(true, 0, "", 0)]
        [TestCase(true, 0, "", 1)]
        [TestCase(true, 1, "", 1)]
        [TestCase(true, 1, "UTCalendar", 0)]
        [TestCase(true, 0, "UTCalendar", 0)]
        [TestCase(true, 0, "UTCalendar", 1)]
        [TestCase(true, 1, "UTCalendar", 1)]
        [TestCase(false, 1, "", 0)]
        [TestCase(false, 0, "", 0)]
        [TestCase(false, 0, "", 1)]
        [TestCase(false, 1, "", 1)]
        [TestCase(false, 1, "UTCalendar", 0)]
        [TestCase(false, 0, "UTCalendar", 0)]
        [TestCase(false, 0, "UTCalendar", 1)]
        [TestCase(false, 1, "UTCalendar", 1)]
        public void RemoveTriggerShouldBeSuccessfully(bool removeNonDurableJob, int triggerGroupLength, string calendarName, int jobTriggerLength)
        {
            var trigger = new TriggerKey(name, @group);

            redis.KeyExistsAsync(schema.TriggerHashKey(trigger)).Returns(true);
            redis.HashGetAllAsync(schema.TriggerHashKey(trigger))
                 .Returns(
                      new[]
                          {
                              new HashEntry(TriggerStoreKey.JobHash, "UnitTestUT:job1"),
                              new HashEntry(TriggerStoreKey.Description, string.Empty),
                              new HashEntry(TriggerStoreKey.NextFireTime, "1522749600000"),
                              new HashEntry(TriggerStoreKey.PrevFireTime, string.Empty),
                              new HashEntry(TriggerStoreKey.Priority, "5"),
                              new HashEntry(TriggerStoreKey.StartTime, "1522749600000"),
                              new HashEntry(TriggerStoreKey.EndTime, string.Empty),
                              new HashEntry(TriggerStoreKey.FinalFireTime, string.Empty),
                              new HashEntry(TriggerStoreKey.FireInstanceId, string.Empty),
                              new HashEntry(TriggerStoreKey.MisfireInstruction, "0"),
                              new HashEntry(TriggerStoreKey.CalendarName, calendarName),
                              new HashEntry(TriggerStoreKey.TriggerType, "Simple"),
                              new HashEntry(TriggerStoreKey.RepeatCount, "0"),
                              new HashEntry(TriggerStoreKey.RepeatInterval, "00:00:00"),
                              new HashEntry(TriggerStoreKey.TimesTriggered, "0")
                          });
            redis.SetLengthAsync(schema.TriggerGroupSetKey(trigger.Group)).Returns(triggerGroupLength);
            redis.SetLengthAsync(schema.JobTriggersKey(schema.JobKey("UnitTestUT:job1"))).Returns(jobTriggerLength);
            redis.HashGetAllAsync(schema.JobHashKey(schema.JobKey("UnitTestUT:job1")))
                 .Returns(RestoreJob());

            Assert.DoesNotThrowAsync(async () => await storage.RemoveTriggerAsync(trigger, removeNonDurableJob));
            redis.Received(1).SetRemove(schema.TriggersKey(), schema.TriggerHashKey(trigger), CommandFlags.FireAndForget);
            redis.Received(1).SetRemove(schema.TriggerGroupSetKey(trigger.Group), schema.TriggerHashKey(trigger), CommandFlags.FireAndForget);
            redis.Received(1).SetRemove(schema.JobTriggersKey(schema.JobKey("UnitTestUT:job1")), schema.TriggerHashKey(trigger), CommandFlags.FireAndForget);
        }

        [Test]
        public async Task RetrieveSimpleTriggerAndTriggerExistShouldBeSuccessfully()
        {
            var trigger = new TriggerKey(name, @group);
            redis.HashGetAllAsync(schema.TriggerHashKey(trigger))
                 .Returns(
                      new[]
                          {
                              new HashEntry(TriggerStoreKey.JobHash, "UnitTestUT:job1"),
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
                              new HashEntry(TriggerStoreKey.TriggerType, "Simple"),
                              new HashEntry(TriggerStoreKey.RepeatCount, "0"),
                              new HashEntry(TriggerStoreKey.RepeatInterval, "00:00:00"),
                              new HashEntry(TriggerStoreKey.TimesTriggered, "0")
                          });
            var result = await storage.RetrieveTriggerAsync(trigger);
            Assert.NotNull(result);
            Assert.IsTrue(result is ISimpleTrigger);
        }

        [Test]
        public async Task RetrieveCronTriggerAndTriggerExistShouldBeSuccessfully()
        {
            var trigger = new TriggerKey(name, @group);
            redis.HashGetAllAsync(schema.TriggerHashKey(trigger)).Returns(RestoreTrigger("UnitTestUT:job1"));
            var result = await storage.RetrieveTriggerAsync(trigger);
            Assert.NotNull(result);
            Assert.IsTrue(result is ICronTrigger);
        }
        
        private void ComputeFirstFireTime(ITrigger trigger)
        {
            var t = (AbstractTrigger)trigger;
            t.ComputeFirstFireTimeUtc(null);
        }

        private double ToUnixTimeMilliseconds(DateTimeOffset dateTimeOffset)
        {
            // Truncate sub-millisecond precision before offsetting by the Unix Epoch to avoid
            // the last digit being off by one for dates that result in negative Unix times
            return (dateTimeOffset - new DateTimeOffset(new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc))).TotalMilliseconds;
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
                           new HashEntry(JobStoreKey.Description, description),
                           new HashEntry(JobStoreKey.RequestRecovery, true),
                           new HashEntry(JobStoreKey.IsDurable, false)
                       };
        }
    }
}