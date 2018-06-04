namespace Quartz.RedisJobStore.Async.UnitTest.JobTests
{
    using System.Threading.Tasks;

    using FluentAssertions;

    using NSubstitute;

    using NUnit.Framework;

    [TestFixture]
    public class CheckExistsTest : FixtureTestBase
    {
        [Test]
        public async Task CheckExistsShouldBeTrue()
        {
            var key = new JobKey("Test", "Unit");
            redis.KeyExistsAsync(schema.RedisJobKey(key)).Returns(true);
            var result = storage.CheckExistsAsync(key);

            (await result).Should().Be(true);
        }

        [Test]
        public async Task CheckExistsShouldBeFalse()
        {
            var key = new JobKey("Test", "Unit");
            redis.KeyExistsAsync(schema.RedisJobKey(key)).Returns(false);
            var result = storage.CheckExistsAsync(key);

            (await result).Should().Be(false);
        }
    }
}