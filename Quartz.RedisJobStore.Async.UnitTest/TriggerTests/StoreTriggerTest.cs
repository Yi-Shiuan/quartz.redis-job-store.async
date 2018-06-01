namespace Quartz.RedisJobStore.Async.UnitTest.TriggerTests
{
    #region

    using System;
    using System.Threading.Tasks;

    using NSubstitute;

    using NUnit.Framework;

    using Quartz.RedisJobStore.Async.Enums;

    using StackExchange.Redis;

    #endregion

    [TestFixture]
    public class StoreTriggerTest : FixtureTestBase
    {
        [Test]
        public async Task StoreTriggerShouldBeStoreToTriggerGroup()
        {
            var trigger = InitializeTestObject();
            var result = storage.StoreTriggerAsync(trigger, true);

            await result;

            redis.Received().SetAdd(schema.RedisTriggerGroupKey(trigger.Key), schema.TriggerStoreKey(trigger.Key), CommandFlags.FireAndForget);
        }

        [Test]
        public async Task StoreTriggerShouldBeStoreToTriggerJobKey()
        {
            var trigger = InitializeTestObject();
            var result = storage.StoreTriggerAsync(trigger, true);

            await result;

            redis.Received().SetAdd(schema.RedisTriggerJobKey(trigger.JobKey), schema.TriggerStoreKey(trigger.Key), CommandFlags.FireAndForget);
        }

        [Test]
        public async Task StoreTriggerShouldBeStoreToTriggerKey()
        {
            var trigger = InitializeTestObject();
            var result = storage.StoreTriggerAsync(trigger, true);

            await result;

            redis.Received().HashSet(schema.RedisTriggerKey(trigger.Key), Arg.Any<HashEntry[]>(), CommandFlags.FireAndForget);
        }

        [Test]
        public async Task StoreTriggerShouldBeStoreToTriggersGroup()
        {
            var trigger = InitializeTestObject();
            var result = storage.StoreTriggerAsync(trigger, true);

            await result;

            redis.Received().SetAdd(schema.RedisTriggerGroupKey(), schema.RedisTriggerGroupKey(trigger.Key), CommandFlags.FireAndForget);
        }

        [Test]
        public async Task StoreTriggerShouldBeStoreToTriggersKey()
        {
            var trigger = InitializeTestObject();
            var result = storage.StoreTriggerAsync(trigger, true);

            await result;

            redis.Received().SetAdd(schema.RedisTriggerKey(), schema.TriggerStoreKey(trigger.Key), CommandFlags.FireAndForget);
        }

        [Test]
        public void StoreDuplicateTriggerShouldBeThrowException()
        {
            var trigger = InitializeTestObject();
            redis.KeyExistsAsync(schema.RedisTriggerKey(trigger.Key)).Returns(true);

            var result = storage.StoreTriggerAsync(trigger, false);

            Assert.ThrowsAsync<ObjectAlreadyExistsException>(async () => await result);
        }

        [Test]
        public void StoreNotSupportTriggerShouldBeThrowException()
        {
            var trigger = Substitute.For<ITrigger>();
            var result = storage.StoreTriggerAsync(trigger, false);

            Assert.ThrowsAsync<NotImplementedException>(async () => await result);
        }

        [Test]
        public async Task StoreTriggerHaveCalenderShouldBeStoreInCalendar()
        {
            var trigger = InitializeTestObject("Calendar1");
            var result = storage.StoreTriggerAsync(trigger, false);

            await result;

            redis.Received().SetAdd(schema.RedisCalendarKey(trigger.CalendarName), schema.TriggerStoreKey(trigger.Key), CommandFlags.FireAndForget);
        }

        [Test]
        public async Task StoreTriggerNotHaveCalenderShouldBeNotStored()
        {
            var trigger = InitializeTestObject();
            var result = storage.StoreTriggerAsync(trigger, false);

            await result;

            redis.DidNotReceive().SetAdd(schema.RedisCalendarKey(trigger.CalendarName), schema.TriggerStoreKey(trigger.Key), CommandFlags.FireAndForget);
        }

        [Test]
        public async Task StoreTriggerIfTriggerAlreadyExistShouldBeRemoveFromAllState()
        {
            var trigger = InitializeTestObject();
            redis.KeyExistsAsync(schema.RedisTriggerKey(trigger.Key)).Returns(true);
            var result = storage.StoreTriggerAsync(trigger, true);

            await result;

            await redis.Received().SortedSetRemoveAsync(schema.RedisTriggerStateKey(TriggerRedisState.Blocked), schema.TriggerStoreKey(trigger.Key));
            await redis.Received().SortedSetRemoveAsync(schema.RedisTriggerStateKey(TriggerRedisState.Acquired), schema.TriggerStoreKey(trigger.Key));
            await redis.Received().SortedSetRemoveAsync(schema.RedisTriggerStateKey(TriggerRedisState.Completed), schema.TriggerStoreKey(trigger.Key));
            await redis.Received().SortedSetRemoveAsync(schema.RedisTriggerStateKey(TriggerRedisState.Error), schema.TriggerStoreKey(trigger.Key));
            await redis.Received().SortedSetRemoveAsync(schema.RedisTriggerStateKey(TriggerRedisState.Paused), schema.TriggerStoreKey(trigger.Key));
            await redis.Received().SortedSetRemoveAsync(schema.RedisTriggerStateKey(TriggerRedisState.PausedBlocked), schema.TriggerStoreKey(trigger.Key));
            await redis.Received().SortedSetRemoveAsync(schema.RedisTriggerStateKey(TriggerRedisState.Waiting), schema.TriggerStoreKey(trigger.Key));
        }

        private ITrigger InitializeTestObject(string cal = "")
        {
            var job = JobBuilder.Create<TestJob>().WithIdentity("Unit", "Test").Build();
            var trigger = TriggerBuilder.Create().ForJob(job).ModifiedByCalendar(cal).Build();
            return trigger;
        }
    }
}