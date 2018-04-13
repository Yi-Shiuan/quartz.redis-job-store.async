namespace Quartz.RedisJobStore.Async.UnitTest
{
    using System.Threading.Tasks;

    using Newtonsoft.Json;
    using Newtonsoft.Json.Serialization;

    using NSubstitute;

    using NUnit.Framework;

    using Quartz.Impl.Calendar;

    using StackExchange.Redis;

    [TestFixture]
    public class CalendarFixtureTest : FixtureTestBase
    {
        private readonly JsonSerializerSettings settings = new JsonSerializerSettings
                                                                          {
                                                                              TypeNameHandling = TypeNameHandling.All,
                                                                              DateTimeZoneHandling =
                                                                                  DateTimeZoneHandling.Utc,
                                                                              NullValueHandling = NullValueHandling.Ignore,
                                                                              ContractResolver =
                                                                                  new
                                                                                      CamelCasePropertyNamesContractResolver()
                                                                          };

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public void StoreCalendarAndCalendarIsExistingWillBeThrow(bool updateTriggers)
        {
            var testCalendarName = "TestCalendar";
            var calendar = new DailyCalendar(0, 0, 0, 0, 23, 59, 59, 999);
            redis.KeyExistsAsync(schema.CalendarHashKey(testCalendarName)).Returns(true);

            Assert.ThrowsAsync<ObjectAlreadyExistsException>(async () => await storage.StoreCalendarAsync(testCalendarName, calendar, false, updateTriggers));
        }

        [Test]
        [TestCase(true, true)]
        [TestCase(true, false)]
        [TestCase(false, true)]
        [TestCase(false, false)]
        public void StoreCalendarSuccessfully(bool replaceExisting, bool updateTriggers)
        {
            var testCalendarName = "TestCalendar";
            var calendar = new DailyCalendar(0, 0, 0, 0, 23, 59, 59, 999);
            redis.KeyExistsAsync(schema.CalendarHashKey(testCalendarName)).Returns(false);
            redis.When(x => x.StringSetAsync(schema.CalendarHashKey(testCalendarName), Arg.Any<string>())).Do(
                info => { Assert.AreEqual(JsonConvert.SerializeObject(calendar, settings), info.Arg<string>()); });

            redis.When(x => x.SetAddAsync(schema.CalendarsKey(), Arg.Any<string>())).Do(
                info => { Assert.AreEqual(schema.CalendarHashKey(testCalendarName), info.Arg<string>()); });

            Assert.DoesNotThrowAsync(async () => await storage.StoreCalendarAsync(testCalendarName, calendar, replaceExisting, updateTriggers));
        }

        [Test]
        public void RemoveCalendarSuccessfully()
        {
            var testCalendarName = "TestCalendar";
            redis.SetLengthAsync(schema.CalendarTriggersKey(testCalendarName)).Returns(0);
            redis.KeyDeleteAsync(schema.CalendarHashKey(testCalendarName)).Returns(true);
            redis.SetRemoveAsync(schema.CalendarsKey(), schema.CalendarHashKey(testCalendarName)).Returns(true);

            Assert.DoesNotThrowAsync(async () => await storage.RemoveCalendarAsync(testCalendarName));
        }

        [Test]
        public void RemoveCalendarWillBeThrow()
        {
            var testCalendarName = "TestCalendar";
            redis.SetLengthAsync(schema.CalendarTriggersKey(testCalendarName)).Returns(1);

            Assert.ThrowsAsync<JobPersistenceException>(async () => await storage.RemoveCalendarAsync(testCalendarName));
        }

        [Test]
        public async Task RetrieveCalendarButCalendarNotInRedis()
        {
            var testCalendarName = "TestCalendar";
            redis.StringGetAsync(schema.CalendarHashKey(testCalendarName)).Returns(new RedisValue());
            var result = storage.RetrieveCalendarAsync(testCalendarName);
            Assert.Null(await result);
        }

        [Test]
        public async Task RetrieveCalendarSuccessfully()
        {
            var testCalendarName = "TestCalendar";
            redis.StringGetAsync(schema.CalendarHashKey(testCalendarName))
                 .Returns(JsonConvert.SerializeObject(new DailyCalendar(0, 0, 0, 0, 23, 59, 59, 999), settings));
            var result = storage.RetrieveCalendarAsync(testCalendarName);
            Assert.NotNull(await result);
        }

        [Test]
        public async Task CalendarNameAsyncSuccessfully()
        {
            redis.SetMembersAsync(schema.CalendarsKey())
                 .Returns(
                      new RedisValue[]
                          {
                              schema.CalendarHashKey("Calendar1"),
                              schema.CalendarHashKey("Calendar2"),
                              schema.CalendarHashKey("Calendar3")
                          });

            var result = storage.CalendarNamesAsync();
            Assert.AreEqual(3, (await result).Count);
        }
    }
}