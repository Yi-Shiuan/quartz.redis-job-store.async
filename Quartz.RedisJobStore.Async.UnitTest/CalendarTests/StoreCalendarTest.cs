namespace Quartz.RedisJobStore.Async.UnitTest.CalendarTests
{
    using System;
    using System.Threading.Tasks;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Serialization;
    using NSubstitute;
    using NUnit.Framework;
    using Quartz.Impl.Calendar;
    using StackExchange.Redis;

    [TestFixture]
    public class StoreCalendarTest : FixtureTestBase
    {
        private readonly JsonSerializerSettings serializer = new JsonSerializerSettings
        {
            TypeNameHandling = TypeNameHandling.All,
            DateTimeZoneHandling = DateTimeZoneHandling.Utc,
            NullValueHandling = NullValueHandling.Ignore,
            ContractResolver = new CamelCasePropertyNamesContractResolver()
        };

        private void InitTestCase(bool exist)
        {
            redis.KeyExistsAsync(schema.RedisCalendarKey("Schedule")).Returns(exist);
        }

        [Test]
        public async Task StoreCalendarShouldBeInsertRedisCalendarSet()
        {
            InitTestCase(true);

            var calendar = new DailyCalendar(DateTime.Today, DateTime.MaxValue);

            await storage.StoreCalendarAsync("Schedule", calendar, true, true);
            redis.Received().SetAdd(schema.RedisCalendarKey(), "Schedule", CommandFlags.FireAndForget);
        }

        [Test]
        public async Task StoreCalendarShouldBeSuccessfully()
        {
            InitTestCase(false);
            var calendar = new DailyCalendar(DateTime.Today, DateTime.MaxValue);

            await storage.StoreCalendarAsync("Schedule", calendar, true, true);

            redis.Received().StringSet(schema.RedisCalendarKey("Schedule"), JsonConvert.SerializeObject(calendar, serializer),
                flags: CommandFlags.FireAndForget);
        }

        [Test]
        public async Task StoreDuplicateCalendarShouldBeSuccessfuly()
        {
            InitTestCase(true);

            var calendar = new DailyCalendar(DateTime.Today, DateTime.MaxValue);

            await storage.StoreCalendarAsync("Schedule", calendar, true, true);
            redis.Received().StringSet(schema.RedisCalendarKey("Schedule"), JsonConvert.SerializeObject(calendar, serializer),
                flags: CommandFlags.FireAndForget);
        }

        [Test]
        public void StoreDuplicateCalendarShouldBeThrow()
        {
            InitTestCase(true);

            var calendar = new DailyCalendar(DateTime.Today, DateTime.MaxValue);

            Assert.ThrowsAsync<ObjectAlreadyExistsException>(async () => await storage.StoreCalendarAsync("Schedule", calendar, false, true));
        }
    }
}