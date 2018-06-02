using System.Threading.Tasks;
using FluentAssertions;
using NSubstitute;
using NUnit.Framework;
using Quartz.Impl.Matchers;
using StackExchange.Redis;

namespace Quartz.RedisJobStore.Async.UnitTest.TriggerTests
{
    [TestFixture]
    public class GetTriggerKeysTest : FixtureTestBase
    {
        [Test]
        public async Task GetTriggerShouldHaveReturn()
        {
            redis.SetMembersAsync(schema.RedisTriggerGroupKey()).Returns(new RedisValue[0]);

            var result = storage.GetTriggerKeysAsync(GroupMatcher<TriggerKey>.AnyGroup());

            (await result).Should().HaveCount(0);
        }
        
        [Test]
        public async Task GetTriggerShouldHaveOneResultReturn()
        {
            ReturnTriggerGroup("UnitTest");
            ReturnTriggerGroupTriggers("UnitTest", "UnitTest:TestTrigger");
            var result = storage.GetTriggerKeysAsync(GroupMatcher<TriggerKey>.AnyGroup());

            (await result).Should().HaveCount(1);
        }
        
        [Test]
        public async Task GetTriggerShouldBeSameTriggerKey()
        {
            var trigger = new TriggerKey("TestTrigger", "UnitTest");
            ReturnTriggerGroup(schema.RedisTriggerGroupKey(trigger));
            ReturnTriggerGroupTriggers(schema.RedisTriggerGroupKey(trigger), schema.TriggerStoreKey(trigger));
            var result = storage.GetTriggerKeysAsync(GroupMatcher<TriggerKey>.AnyGroup());

            (await result).Should().HaveCount(1);
            (await result).Should().Contain(trigger);
        }
        
        [Test]
        public async Task GetTriggerStartWithTestShouldHaveOneResult()
        {
            ReturnTriggerGroup("TestTrigger", "UnitTest");
            ReturnTriggerGroupTriggers("TestTrigger", "TestTrigger:trigger1");
            ReturnTriggerGroupTriggers("UnitTest", "UnitTest:trigger1");
            
            var result = storage.GetTriggerKeysAsync(GroupMatcher<TriggerKey>.GroupStartsWith("Test"));

            (await result).Should().HaveCount(1);
            (await result).Should().Contain(new TriggerKey("trigger1", "TestTrigger"));
        }
        
        [Test]
        public async Task GetTriggerEndWithTestShouldHaveOneResult()
        {
            ReturnTriggerGroup("TestTrigger", "UnitTest");
            ReturnTriggerGroupTriggers("TestTrigger", "TestTrigger:trigger1");
            ReturnTriggerGroupTriggers("UnitTest", "UnitTest:trigger1");
            
            var result = storage.GetTriggerKeysAsync(GroupMatcher<TriggerKey>.GroupEndsWith("Test"));

            (await result).Should().HaveCount(1);
            (await result).Should().Contain(new TriggerKey("trigger1", "UnitTest"));
        }
        
        [Test]
        public async Task GetTriggerEqualsWithTestShouldHaveOneResult()
        {
            ReturnTriggerGroup("TestTrigger", "UnitTest");
            ReturnTriggerGroupTriggers("TestTrigger", "TestTrigger:trigger1");
            ReturnTriggerGroupTriggers("UnitTest", "UnitTest:trigger1");
            
            var result = storage.GetTriggerKeysAsync(GroupMatcher<TriggerKey>.GroupEquals("TestTrigger"));

            (await result).Should().HaveCount(1);
            (await result).Should().Contain(new TriggerKey("trigger1", "TestTrigger"));
        }
        
        [Test]
        public async Task GetTriggerContainsWithTestShouldHaveOneResult()
        {
            ReturnTriggerGroup("TestTrigger", "UnitTest");
            ReturnTriggerGroupTriggers("TestTrigger", "TestTrigger:trigger1");
            ReturnTriggerGroupTriggers("UnitTest", "UnitTest:trigger1");
            
            var result = storage.GetTriggerKeysAsync(GroupMatcher<TriggerKey>.GroupContains("Test"));

            (await result).Should().HaveCount(2);
        }
        
        
        
        private void ReturnTriggerGroup(params RedisValue[] values)
        {
            redis.SetMembersAsync(schema.RedisTriggerGroupKey()).Returns(values);
        }

        private void ReturnTriggerGroupTriggers(string key, params RedisValue[] values)
        {
            redis.SetMembersAsync(schema.RedisTriggerGroupKey(key)).Returns(values);
        }
    }
}