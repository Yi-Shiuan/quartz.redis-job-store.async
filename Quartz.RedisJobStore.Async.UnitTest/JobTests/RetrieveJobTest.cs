using System.Threading.Tasks;
using FluentAssertions;
using NSubstitute;
using NUnit.Framework;
using Quartz.RedisJobStore.Async.Enums;
using StackExchange.Redis;

namespace Quartz.RedisJobStore.Async.UnitTest.JobTests
{
    #region

    #endregion

    [TestFixture]
    public class RetrieveJobTest : FixtureTestBase
    {
        private JobKey InitializeTestCase()
        {
            var key = new JobKey("Unit", "Test");
            redis.HashGetAllAsync(schema.RedisJobKey(key)).Returns(SetRedisJobData());
            return key;
        }

        private HashEntry[] SetRedisJobData()
        {
            return new[]
            {
                new HashEntry(JobStoreKey.JobClass, typeof(TestJob).AssemblyQualifiedName),
                new HashEntry(JobStoreKey.Description, string.Empty),
                new HashEntry(JobStoreKey.RequestRecovery, bool.TrueString),
                new HashEntry(JobStoreKey.IsDurable, bool.FalseString),
                new HashEntry(JobStoreKey.BlockedBy, string.Empty),
                new HashEntry(JobStoreKey.BlockTime, string.Empty)
            };
        }

        private HashEntry[] SetRedisJobDataMap()
        {
            return new[]
            {
                new HashEntry("TestCase", "test")
            };
        }

        [Test]
        public async Task RedisJobDataIsEmptyDataMapShouldBeEmpty()
        {
            var key = InitializeTestCase();
            redis.HashGetAllAsync(schema.RedisJobDataMap(key)).Returns(new HashEntry[0]);
            var result = storage.RetrieveJobAsync(key);

            (await result).JobDataMap.Should().BeEmpty();
        }

        [Test]
        public async Task RedisJobDataIsNotEmptyDataMapShouldNotBeEmpty()
        {
            var key = InitializeTestCase();
            redis.HashGetAllAsync(schema.RedisJobDataMap(key)).Returns(SetRedisJobDataMap());
            var result = storage.RetrieveJobAsync(key);

            (await result).JobDataMap.Should().NotBeEmpty();
        }

        [Test]
        public async Task RedisReturnEmptyShouldBeReturnNull()
        {
            var key = new JobKey("Unit", "Test");
            redis.HashGetAllAsync(schema.RedisJobKey(key)).Returns(new HashEntry[0]);
            var result = storage.RetrieveJobAsync(key);

            (await result).Should().BeNull();
        }

        [Test]
        public async Task RedisReturnRecordShouldBeEqualRedisData()
        {
            var key = InitializeTestCase();
            var result = storage.RetrieveJobAsync(key);

            (await result).Description.Should().Be(string.Empty);
            (await result).Key.Should().Be(key);
            (await result).Durable.Should().Be(false);
            (await result).RequestsRecovery.Should().Be(true);
            (await result).JobType.Should().Be(typeof(TestJob));
        }

        [Test]
        public async Task RedisReturnRecordShouldBeReturnJobDetail()
        {
            var key = InitializeTestCase();
            var result = storage.RetrieveJobAsync(key);

            (await result).Should().NotBeNull();
        }
    }
}