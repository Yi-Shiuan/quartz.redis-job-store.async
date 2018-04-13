namespace Quartz.RedisJobStore.Async.UnitTest
{
    #region

    using NSubstitute;

    using NUnit.Framework;

    using Quartz.Spi;

    using StackExchange.Redis;

    #endregion

    public class FixtureTestBase
    {
        protected readonly string Delimiter = ":";

        protected readonly string Prefix = "UnitTest";

        protected IDatabase redis;

        protected RedisKeySchema schema;

        protected RedisStorage storage;

        [SetUp]
        public virtual void Setup()
        {
            redis = Substitute.For<IDatabase>();
            schema = new RedisKeySchema(Delimiter, Prefix);
            storage = Substitute.For<RedisStorage>(schema, redis, Substitute.For<ISchedulerSignaler>(), "UnitTestInstance", 1000, 1000, 60000);
        }
    }
}