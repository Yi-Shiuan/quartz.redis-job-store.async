namespace Quartz.RedisJobStore.Async.UnitTest.JobTests
{
    #region

    using System.Threading.Tasks;

    using FluentAssertions;

    using NSubstitute;

    using NUnit.Framework;

    using Quartz.Impl.Matchers;

    using StackExchange.Redis;

    #endregion

    [TestFixture]
    public class GetJobKeysTest : FixtureTestBase
    {
        [Test]
        public async Task GetAnyJobKeysKeyIsEmpty()
        {
            redis.SetMembersAsync(schema.RedisJobGroupKey()).Returns(new RedisValue[0]);

            var result = storage.GetJobKeysAsync(GroupMatcher<JobKey>.AnyGroup());

            (await result).Should().HaveCount(0);
        }

        [Test]
        public async Task GetAnyJobKeysKeyHaveOneResult()
        {
            ReturnRedisJobGroup("UnitTest");
            ReturnJobGroupJobs("UnitTest", "UnitTest:Case1");

            var result = storage.GetJobKeysAsync(GroupMatcher<JobKey>.AnyGroup());

            (await result).Should().HaveCount(1);
        }

        [Test]
        public async Task GetEndWithTestCase1JobKeysKeyIsEmpty()
        {
            redis.SetMembersAsync(schema.RedisJobGroupKey()).Returns(new RedisValue[0]);

            var result = storage.GetJobKeysAsync(GroupMatcher<JobKey>.GroupEndsWith("TestCase1"));

            (await result).Should().HaveCount(0);
        }

        [Test]
        public async Task GetEndWithTestCase1JobKeysKeyHaveTwoResult()
        {
            ReturnRedisJobGroup("Case1Test", "Case2Test");
            ReturnJobGroupJobs("Case1Test", "Case1Test:Case1");
            ReturnJobGroupJobs("Case2Test", "Case2Test:Case1");

            var result = storage.GetJobKeysAsync(GroupMatcher<JobKey>.GroupEndsWith("Test"));

            (await result).Should().HaveCount(2);
        }

        [Test]
        public async Task GetEndWithTestCase1JobKeysKeyHaveTwoResultAanHaveOneNullKey()
        {
            ReturnRedisJobGroup("Case1Test", "Case2Test");
            ReturnJobGroupJobs("Case1Test", "Case1Test:Case1");
            ReturnJobGroupJobs("Case2Test", "Case2Test:Case1");

            var result = storage.GetJobKeysAsync(GroupMatcher<JobKey>.GroupEndsWith("Test"));

            (await result).Should().HaveCount(2);
        }

        [Test]
        public async Task GetStartWithTestCaseJobKeysKeyHaveOneResult()
        {
            ReturnRedisJobGroup("Case1Test", "T2");
            ReturnJobGroupJobs("Case1Test", "Case1Test:Case1");
            ReturnJobGroupJobs("T2", "T2:Case1");

            var result = storage.GetJobKeysAsync(GroupMatcher<JobKey>.GroupStartsWith("Case"));

            (await result).Should().Contain(x => x.Group.StartsWith("Case"));
        }

        [TestCase("TestCase")]
        public async Task GetGroupIsTestCaseJobKeysKeyHaveOneResult(string @group)
        {
            ReturnRedisJobGroup("TestCase", "Case2Test");
            ReturnJobGroupJobs("TestCase", "TestCase:Case1");
            ReturnJobGroupJobs("Case2Test", "Case2Test:Case1");

            var result = storage.GetJobKeysAsync(GroupMatcher<JobKey>.GroupEquals(@group));

            (await result).Should().Contain(x => x.Group == "TestCase");
        }

        private void ReturnRedisJobGroup(params RedisValue[] values)
        {
            redis.SetMembersAsync(schema.RedisJobGroupKey()).Returns(values);
        }

        private void ReturnJobGroupJobs(string key, params RedisValue[] values)
        {
            redis.SetMembersAsync(schema.RedisJobGroupKey(key)).Returns(values);
        }
    }
}