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
            redis.SortedSetScoreAsync(schema.TriggerStateKey(TriggerStateEnum.Completed), schema.TriggerHashKey(trigger)).Returns(complate);

            Assert.DoesNotThrowAsync(async () => await storage.PauseTriggerAsync(trigger));

            redis.DidNotReceive()
                 .SortedSetAddAsync(schema.TriggerStateKey(TriggerStateEnum.Paused), schema.TriggerHashKey(trigger), Arg.Any<double>());
        }

        [Test]
        [TestCase(null, TriggerStateEnum.Paused)]
        [TestCase(9, TriggerStateEnum.PausedBlocked)]
        public void PauseTriggerShouldBeSuccessfully(double? blocked, TriggerStateEnum state)
        {
            var trigger = new TriggerKey(name, group);

            redis.KeyExistsAsync(schema.TriggerHashKey(trigger)).Returns(true);
            redis.SortedSetScoreAsync(schema.TriggerStateKey(TriggerStateEnum.Completed), schema.TriggerHashKey(trigger)).Returns((double?)null);
            redis.SortedSetScoreAsync(schema.TriggerStateKey(TriggerStateEnum.Blocked), schema.TriggerHashKey(trigger)).Returns(blocked);
            redis.HashGetAsync(schema.TriggerHashKey(trigger), TriggerStoreKeyEnum.NextFireTime).Returns(999999);

            Assert.DoesNotThrowAsync(async () => await storage.PauseTriggerAsync(trigger));

            redis.Received().SortedSetAddAsync(schema.TriggerStateKey(state), schema.TriggerHashKey(trigger), Arg.Any<double>());
        }

        [Test]
        public void ResumeTriggerButTriggerNotPauseShouldBeNotThrow()
        {
            var trigger = new TriggerKey(name, group);

            redis.SortedSetScoreAsync(schema.TriggerStateKey(TriggerStateEnum.Paused), schema.TriggerHashKey(trigger)).Returns((double?)null);
            redis.SortedSetScoreAsync(schema.TriggerStateKey(TriggerStateEnum.PausedBlocked), schema.TriggerHashKey(trigger)).Returns((double?)null);

            Assert.DoesNotThrowAsync(async () => await storage.ResumeTriggerAsync(trigger));

            redis.DidNotReceive()
                 .SortedSetAddAsync(schema.TriggerStateKey(TriggerStateEnum.Waiting), schema.TriggerHashKey(trigger), Arg.Any<double>());
            redis.DidNotReceive()
                 .SortedSetAddAsync(schema.TriggerStateKey(TriggerStateEnum.Blocked), schema.TriggerHashKey(trigger), Arg.Any<double>());
        }

        [Test]
        [TestCase(null, 0, true, TriggerStateEnum.Blocked)]
        [TestCase(0, null, false, TriggerStateEnum.Waiting)]
        public void ResumeTriggerShouldBeSuccessfully(double? paused, double? pauseBlocked, bool isBlockedJob, TriggerStateEnum state)
        {
            var trigger = new TriggerKey(name, group);

            redis.SortedSetScoreAsync(schema.TriggerStateKey(TriggerStateEnum.Paused), schema.TriggerHashKey(trigger)).Returns(paused);
            redis.SortedSetScoreAsync(schema.TriggerStateKey(TriggerStateEnum.PausedBlocked), schema.TriggerHashKey(trigger)).Returns(pauseBlocked);
            redis.HashGetAsync(schema.TriggerHashKey(trigger), TriggerStoreKeyEnum.NextFireTime).Returns(999999);
            redis.SetContainsAsync(schema.BlockedJobsSet(), schema.JobHashKey(new JobKey("Job", "UT"))).Returns(isBlockedJob);

            redis.HashGetAllAsync(schema.TriggerHashKey(trigger))
                 .Returns(
                      new[]
                          {
                              new HashEntry(TriggerStoreKeyEnum.JobHash, schema.JobHashKey(new JobKey("Job", "UT"))),
                              new HashEntry(TriggerStoreKeyEnum.Description, string.Empty),
                              new HashEntry(TriggerStoreKeyEnum.NextFireTime, "1522749600000"),
                              new HashEntry(TriggerStoreKeyEnum.PrevFireTime, string.Empty),
                              new HashEntry(TriggerStoreKeyEnum.Priority, "5"),
                              new HashEntry(TriggerStoreKeyEnum.StartTime, "1522749600000"),
                              new HashEntry(TriggerStoreKeyEnum.EndTime, string.Empty),
                              new HashEntry(TriggerStoreKeyEnum.FinalFireTime, string.Empty),
                              new HashEntry(TriggerStoreKeyEnum.FireInstanceId, string.Empty),
                              new HashEntry(TriggerStoreKeyEnum.MisfireInstruction, "0"),
                              new HashEntry(TriggerStoreKeyEnum.CalendarName, string.Empty),
                              new HashEntry(TriggerStoreKeyEnum.TriggerType, "Simple"),
                              new HashEntry(TriggerStoreKeyEnum.RepeatCount, "0"),
                              new HashEntry(TriggerStoreKeyEnum.RepeatInterval, "00:00:00"),
                              new HashEntry(TriggerStoreKeyEnum.TimesTriggered, "0")
                          });

            Assert.DoesNotThrowAsync(async () => await storage.ResumeTriggerAsync(trigger));

            redis.Received().SortedSetAddAsync(schema.TriggerStateKey(state), schema.TriggerHashKey(trigger), Arg.Any<double>());
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
                              Assert.NotNull(hashSet[TriggerStoreKeyEnum.JobHash]);
                              Assert.NotNull(hashSet[TriggerStoreKeyEnum.Description]);
                              Assert.NotNull(hashSet[TriggerStoreKeyEnum.NextFireTime]);
                              Assert.NotNull(hashSet[TriggerStoreKeyEnum.PrevFireTime]);
                              Assert.NotNull(hashSet[TriggerStoreKeyEnum.Priority]);
                              Assert.NotNull(hashSet[TriggerStoreKeyEnum.StartTime]);
                              Assert.NotNull(hashSet[TriggerStoreKeyEnum.EndTime]);
                              Assert.NotNull(hashSet[TriggerStoreKeyEnum.FinalFireTime]);
                              Assert.NotNull(hashSet[TriggerStoreKeyEnum.FireInstanceId]);
                              Assert.NotNull(hashSet[TriggerStoreKeyEnum.MisfireInstruction]);
                              Assert.NotNull(hashSet[TriggerStoreKeyEnum.CalendarName]);
                              Assert.NotNull(hashSet[TriggerStoreKeyEnum.TriggerType]);
                              Assert.NotNull(hashSet[TriggerStoreKeyEnum.RepeatCount]);
                              Assert.NotNull(hashSet[TriggerStoreKeyEnum.RepeatInterval]);
                              Assert.NotNull(hashSet[TriggerStoreKeyEnum.TimesTriggered]);
                          });

            Assert.DoesNotThrowAsync(async () => await storage.StoreTriggerAsync(trigger, true));
            redis.Received(1).SetAdd(schema.TriggersKey(), schema.TriggerHashKey(trigger.Key), CommandFlags.FireAndForget);
            redis.Received().SortedSetRemoveAsync(schema.TriggerStateKey(Arg.Any<TriggerStateEnum>()), schema.TriggerHashKey(trigger.Key));
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
                              Assert.NotNull(hashSet[TriggerStoreKeyEnum.JobHash]);
                              Assert.NotNull(hashSet[TriggerStoreKeyEnum.Description]);
                              Assert.NotNull(hashSet[TriggerStoreKeyEnum.NextFireTime]);
                              Assert.NotNull(hashSet[TriggerStoreKeyEnum.PrevFireTime]);
                              Assert.NotNull(hashSet[TriggerStoreKeyEnum.Priority]);
                              Assert.NotNull(hashSet[TriggerStoreKeyEnum.StartTime]);
                              Assert.NotNull(hashSet[TriggerStoreKeyEnum.EndTime]);
                              Assert.NotNull(hashSet[TriggerStoreKeyEnum.FinalFireTime]);
                              Assert.NotNull(hashSet[TriggerStoreKeyEnum.FireInstanceId]);
                              Assert.NotNull(hashSet[TriggerStoreKeyEnum.MisfireInstruction]);
                              Assert.NotNull(hashSet[TriggerStoreKeyEnum.CalendarName]);
                              Assert.NotNull(hashSet[TriggerStoreKeyEnum.TriggerType]);
                              Assert.NotNull(hashSet[TriggerStoreKeyEnum.RepeatCount]);
                              Assert.NotNull(hashSet[TriggerStoreKeyEnum.RepeatInterval]);
                              Assert.NotNull(hashSet[TriggerStoreKeyEnum.TimesTriggered]);
                          });

            Assert.DoesNotThrowAsync(async () => await storage.StoreTriggerAsync(trigger, replaceExisting));
            redis.Received(1).SetAdd(schema.TriggersKey(), schema.TriggerHashKey(trigger.Key), CommandFlags.FireAndForget);
            redis.Received().SortedSetRemoveAsync(schema.TriggerStateKey(Arg.Any<TriggerStateEnum>()), schema.TriggerHashKey(trigger.Key));
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
                              new HashEntry(TriggerStoreKeyEnum.JobHash, "UnitTest:UT:job1"),
                              new HashEntry(TriggerStoreKeyEnum.Description, string.Empty),
                              new HashEntry(TriggerStoreKeyEnum.NextFireTime, "1522749600000"),
                              new HashEntry(TriggerStoreKeyEnum.PrevFireTime, string.Empty),
                              new HashEntry(TriggerStoreKeyEnum.Priority, "5"),
                              new HashEntry(TriggerStoreKeyEnum.StartTime, "1522749600000"),
                              new HashEntry(TriggerStoreKeyEnum.EndTime, string.Empty),
                              new HashEntry(TriggerStoreKeyEnum.FinalFireTime, string.Empty),
                              new HashEntry(TriggerStoreKeyEnum.FireInstanceId, string.Empty),
                              new HashEntry(TriggerStoreKeyEnum.MisfireInstruction, "0"),
                              new HashEntry(TriggerStoreKeyEnum.CalendarName, calendarName),
                              new HashEntry(TriggerStoreKeyEnum.TriggerType, "Simple"),
                              new HashEntry(TriggerStoreKeyEnum.RepeatCount, "0"),
                              new HashEntry(TriggerStoreKeyEnum.RepeatInterval, "00:00:00"),
                              new HashEntry(TriggerStoreKeyEnum.TimesTriggered, "0")
                          });
            redis.SetLengthAsync(schema.TriggerGroupSetKey(trigger.Group)).Returns(triggerGroupLength);
            redis.SetLengthAsync(schema.JobTriggersKey(schema.JobKey("UnitTest:UT:job1"))).Returns(jobTriggerLength);
            redis.HashGetAllAsync(schema.JobHashKey(schema.JobKey("UnitTest:UT:job1")))
                 .Returns(
                      new[]
                          {
                              new HashEntry(JobStoreKeyEnum.JobClass, typeof(TestJob).AssemblyQualifiedName),
                              new HashEntry(JobStoreKeyEnum.Description, description),
                              new HashEntry(JobStoreKeyEnum.RequestRecovery, true),
                              new HashEntry(JobStoreKeyEnum.IsDurable, false)
                          });

            Assert.DoesNotThrowAsync(async () => await storage.RemoveTriggerAsync(trigger, removeNonDurableJob));
            redis.Received(1).SetRemove(schema.TriggersKey(), schema.TriggerHashKey(trigger), CommandFlags.FireAndForget);
            redis.Received(1).SetRemove(schema.TriggerGroupSetKey(trigger.Group), schema.TriggerHashKey(trigger), CommandFlags.FireAndForget);
            redis.Received(1).SetRemove(schema.JobTriggersKey(schema.JobKey("UnitTest:UT:job1")), schema.TriggerHashKey(trigger), CommandFlags.FireAndForget);
        }

        [Test]
        public async Task RetrieveSimpleTriggerAndTriggerExistShouldBeSuccessfully()
        {
            var trigger = new TriggerKey(name, @group);
            redis.HashGetAllAsync(schema.TriggerHashKey(trigger))
                 .Returns(
                      new[]
                          {
                              new HashEntry(TriggerStoreKeyEnum.JobHash, "UnitTest:UT:job1"),
                              new HashEntry(TriggerStoreKeyEnum.Description, string.Empty),
                              new HashEntry(TriggerStoreKeyEnum.NextFireTime, "1522749600000"),
                              new HashEntry(TriggerStoreKeyEnum.PrevFireTime, string.Empty),
                              new HashEntry(TriggerStoreKeyEnum.Priority, "5"),
                              new HashEntry(TriggerStoreKeyEnum.StartTime, "1522749600000"),
                              new HashEntry(TriggerStoreKeyEnum.EndTime, string.Empty),
                              new HashEntry(TriggerStoreKeyEnum.FinalFireTime, string.Empty),
                              new HashEntry(TriggerStoreKeyEnum.FireInstanceId, string.Empty),
                              new HashEntry(TriggerStoreKeyEnum.MisfireInstruction, "0"),
                              new HashEntry(TriggerStoreKeyEnum.CalendarName, string.Empty),
                              new HashEntry(TriggerStoreKeyEnum.TriggerType, "Simple"),
                              new HashEntry(TriggerStoreKeyEnum.RepeatCount, "0"),
                              new HashEntry(TriggerStoreKeyEnum.RepeatInterval, "00:00:00"),
                              new HashEntry(TriggerStoreKeyEnum.TimesTriggered, "0")
                          });
            var result = await storage.RetrieveTriggerAsync(trigger);
            Assert.NotNull(result);
            Assert.IsTrue(result is ISimpleTrigger);
        }

        [Test]
        public async Task RetrieveCronTriggerAndTriggerExistShouldBeSuccessfully()
        {
            var trigger = new TriggerKey(name, @group);
            redis.HashGetAllAsync(schema.TriggerHashKey(trigger))
                 .Returns(
                      new[]
                          {
                              new HashEntry(TriggerStoreKeyEnum.JobHash, "UnitTest:UT:job1"),
                              new HashEntry(TriggerStoreKeyEnum.Description, string.Empty),
                              new HashEntry(TriggerStoreKeyEnum.NextFireTime, "1522749600000"),
                              new HashEntry(TriggerStoreKeyEnum.PrevFireTime, string.Empty),
                              new HashEntry(TriggerStoreKeyEnum.Priority, "5"),
                              new HashEntry(TriggerStoreKeyEnum.StartTime, "1522749600000"),
                              new HashEntry(TriggerStoreKeyEnum.EndTime, string.Empty),
                              new HashEntry(TriggerStoreKeyEnum.FinalFireTime, string.Empty),
                              new HashEntry(TriggerStoreKeyEnum.FireInstanceId, string.Empty),
                              new HashEntry(TriggerStoreKeyEnum.MisfireInstruction, "0"),
                              new HashEntry(TriggerStoreKeyEnum.CalendarName, string.Empty),
                              new HashEntry(TriggerStoreKeyEnum.TriggerType, "CRON"),
                              new HashEntry(TriggerStoreKeyEnum.TimeZoneId, TimeZoneInfo.Utc.Id),
                              new HashEntry(TriggerStoreKeyEnum.CronExpression, "0 0 */3 ? * * *"),
                          });
            var result = await storage.RetrieveTriggerAsync(trigger);
            Assert.NotNull(result);
            Assert.IsTrue(result is ICronTrigger);
        }

        private void ComputeFirstFireTime(ITrigger trigger)
        {
            var t = (AbstractTrigger)trigger;
            t.ComputeFirstFireTimeUtc(null);
        }
    }
}