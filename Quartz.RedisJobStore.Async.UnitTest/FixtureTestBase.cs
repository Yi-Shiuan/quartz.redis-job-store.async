using NSubstitute;
using NUnit.Framework;
using Quartz.Spi;
using StackExchange.Redis;

namespace Quartz.RedisJobStore.Async.UnitTest
{
    #region

    #endregion

    public class FixtureTestBase
    {
        protected readonly string Delimiter = ":";

        protected IDatabase redis;

        protected RedisKeySchema schema;

        protected RedisStorage storage;

        [SetUp]
        public virtual void Setup()
        {
            redis = Substitute.For<IDatabase>();
            schema = new RedisKeySchema(Delimiter);
            storage = new RedisStorage(schema, redis, Substitute.For<ISchedulerSignaler>(), "UnitTest", 60000);
        }
    }
}