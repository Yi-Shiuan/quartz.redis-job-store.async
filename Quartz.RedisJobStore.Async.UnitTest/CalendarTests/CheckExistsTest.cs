using System.Threading.Tasks;
using FluentAssertions;
using NSubstitute;
using NUnit.Framework;

namespace Quartz.RedisJobStore.Async.UnitTest.CalendarTests
{
    #region

    #endregion

    [TestFixture]
    public class CheckExistsTest : FixtureTestBase
    {
        private string InitTest(bool value)
        {
            var calName = "UnitTest";
            redis.KeyExistsAsync(schema.RedisCalendarKey(calName)).Returns(value);
            return calName;
        }

        [Test]
        public async Task CheckCalendarShouldBeFalse()
        {
            var calName = InitTest(false);
            var result = storage.CheckExistsAsync(calName);

            (await result).Should().Be(false);
        }

        [Test]
        public async Task CheckCalendarShouldBeTrue()
        {
            var calName = InitTest(true);
            var result = storage.CheckExistsAsync(calName);

            (await result).Should().Be(true);
        }
    }
}