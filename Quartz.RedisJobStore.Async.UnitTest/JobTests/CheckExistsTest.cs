﻿namespace Quartz.RedisJobStore.Async.UnitTest.JobTests
{
    #region

    using System.Threading.Tasks;

    using FluentAssertions;

    using NSubstitute;

    using NUnit.Framework;

    #endregion

    [TestFixture]
    public class CheckExistsTest : FixtureTestBase
    {
        [Test]
        public async Task CheckExistsShouldBeFalse()
        {
            var key = InitTest(false);
            var result = storage.CheckExistsAsync(key);

            (await result).Should().Be(false);
        }

        [Test]
        public async Task CheckExistsShouldBeTrue()
        {
            var key = InitTest(true);
            var result = storage.CheckExistsAsync(key);

            (await result).Should().Be(true);
        }

        private JobKey InitTest(bool value)
        {
            var key = new JobKey("Test", "Unit");
            redis.KeyExistsAsync(schema.RedisJobKey(key)).Returns(value);
            return key;
        }
    }
}