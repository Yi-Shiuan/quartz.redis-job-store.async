using System.Threading.Tasks;
using FluentAssertions;
using NSubstitute;
using NUnit.Framework;

namespace Quartz.RedisJobStore.Async.UnitTest.TriggerTests
{
    #region

    #endregion

    [TestFixture]
    public class CheckExistsTest : FixtureTestBase
    {
        private TriggerKey InitTriggerKey(bool value)
        {
            var key = new TriggerKey("Test", "Unit");
            redis.KeyExistsAsync(schema.RedisTriggerKey(key)).Returns(value);
            return key;
        }

        [Test]
        public async Task CheckExistsShouldBeFalse()
        {
            var key = InitTriggerKey(false);
            var result = storage.CheckExistsAsync(key);

            (await result).Should().Be(false);
        }

        [Test]
        public async Task CheckExistsShouldBeTrue()
        {
            var key = InitTriggerKey(true);
            var result = storage.CheckExistsAsync(key);

            (await result).Should().Be(true);
        }
    }
}