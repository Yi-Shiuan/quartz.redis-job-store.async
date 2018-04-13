namespace Quartz.RedisJobStore.Async.UnitTest
{
    #region

    using System.Threading.Tasks;

    using NSubstitute;

    using NUnit.Framework;

    using Quartz.Impl.Triggers;
    using Quartz.RedisJobStore.Async.Enums;
    using Quartz.Spi;

    using StackExchange.Redis;

    #endregion

    [TestFixture]
    public class JobFixtureTest : FixtureTestBase
    {
        private string description = "UnitTest";

        [Test]
        public async Task RetrieveJobShouldBeNull()
        {
            var key = new JobKey("Test", "Unit");

            redis.HashGetAllAsync(schema.JobHashKey(key)).Returns(new HashEntry[0]);

            var job = await storage.RetrieveJobAsync(key);

            Assert.Null(job);
        }

        [Test]
        public async Task RetrieveJobShouldBeSuccess()
        {
            var key = new JobKey("Test", "Unit");

            redis.HashGetAllAsync(schema.JobHashKey(key))
                 .Returns(
                      new[]
                          {
                              new HashEntry(JobStoreKeyEnum.JobClass, typeof(TestJob).AssemblyQualifiedName),
                              new HashEntry(JobStoreKeyEnum.Description, description),
                              new HashEntry(JobStoreKeyEnum.RequestRecovery, true),
                              new HashEntry(JobStoreKeyEnum.IsDurable, false)
                          });

            redis.HashGetAllAsync(schema.JobDataMapHashKey(key))
                 .Returns(
                      new[]
                          {
                              new HashEntry("Test", "Unit")
                          });

            var job = await storage.RetrieveJobAsync(key);

            Assert.NotNull(job);
            Assert.AreEqual(1, job.JobDataMap.Count);
        }

        [Test]
        public async Task RetrieveJobWithoutDataShouldBeSuccess()
        {
            var key = new JobKey("Test", "Unit");

            redis.HashGetAllAsync(schema.JobHashKey(key))
                 .Returns(
                      new[]
                          {
                              new HashEntry(JobStoreKeyEnum.JobClass, typeof(TestJob).AssemblyQualifiedName),
                              new HashEntry(JobStoreKeyEnum.Description, description),
                              new HashEntry(JobStoreKeyEnum.RequestRecovery, true),
                              new HashEntry(JobStoreKeyEnum.IsDurable, false)
                          });

            var job = await storage.RetrieveJobAsync(key);

            Assert.NotNull(job);
            Assert.AreEqual(0, job.JobDataMap.Count);
        }

        [Test]
        [TestCase("replace", "")]
        [TestCase("Unit", "This is for unit test")]
        public void StoreJobButJobExistShouldBeOverride(string data, string des)
        {
            description = string.IsNullOrEmpty(des) ? description : des;
            var job = JobBuilder.Create<TestJob>().WithIdentity("Test", "Unit").WithDescription(description).UsingJobData("unit", data).Build();

            redis.When(x => x.HashSetAsync(schema.JobHashKey(job.Key), Arg.Any<HashEntry[]>()))
                 .Do(
                      info =>
                          {
                              var hash = info.Arg<HashEntry[]>().ToStringDictionary();
                              Assert.AreEqual(6, hash.Count);
                              Assert.AreEqual(description, hash[JobStoreKeyEnum.Description]);
                              Assert.AreEqual(typeof(TestJob).AssemblyQualifiedName, hash[JobStoreKeyEnum.JobClass]);
                              Assert.AreEqual("0", hash[JobStoreKeyEnum.IsDurable]);
                              Assert.AreEqual("0", hash[JobStoreKeyEnum.RequestRecovery]);
                              Assert.IsEmpty(hash[JobStoreKeyEnum.BlockedBy]);
                              Assert.IsEmpty(hash[JobStoreKeyEnum.BlockTime]);
                          });

            redis.When(x => x.HashSetAsync(schema.JobDataMapHashKey(job.Key), Arg.Any<HashEntry[]>()))
                 .Do(
                      info =>
                          {
                              var hash = info.Arg<HashEntry[]>().ToStringDictionary();
                              Assert.AreEqual(1, hash.Count);
                              Assert.AreEqual(data, hash["unit"]);
                          });
            redis.When(x => x.SetAddAsync(schema.JobsKey(), Arg.Any<RedisValue>()))
                 .Do(info => { Assert.AreEqual(schema.JobHashKey(job.Key), info.Arg<RedisValue>().ToString()); });

            redis.When(x => x.SetAddAsync(schema.JobGroupsKey(), Arg.Any<RedisValue>()))
                 .Do(info => { Assert.AreEqual(schema.JobGroupKey(job.Key.Group), info.Arg<RedisValue>().ToString()); });

            redis.When(x => x.SetAddAsync(schema.JobGroupKey(job.Key.Group), Arg.Any<RedisValue>()))
                 .Do(info => { Assert.AreEqual(schema.JobHashKey(job.Key), info.Arg<RedisValue>().ToString()); });

            redis.KeyExistsAsync(schema.JobHashKey(job.Key)).Returns(true);

            Assert.DoesNotThrowAsync(async () => await storage.StoreJobAsync(job, true));
        }

        [Test]
        public void StoreJobButJobExistShouldBeThrow()
        {
            var job = JobBuilder.Create<TestJob>().WithIdentity("Test", "Unit").WithDescription(description).UsingJobData("unit", "test").Build();

            redis.KeyExistsAsync(schema.JobHashKey(job.Key)).Returns(true);

            Assert.ThrowsAsync<ObjectAlreadyExistsException>(async () => await storage.StoreJobAsync(job, false));
        }

        [Test]
        public void StoreJobShouldBeSuccess()
        {
            var job = JobBuilder.Create<TestJob>().WithIdentity("Test", "Unit").WithDescription(description).UsingJobData("unit", "test").Build();

            redis.When(x => x.HashSetAsync(schema.JobHashKey(job.Key), Arg.Any<HashEntry[]>()))
                 .Do(
                      info =>
                          {
                              var hash = info.Arg<HashEntry[]>().ToStringDictionary();
                              Assert.AreEqual(6, hash.Count);
                              Assert.AreEqual(description, hash[JobStoreKeyEnum.Description]);
                              Assert.AreEqual(typeof(TestJob).AssemblyQualifiedName, hash[JobStoreKeyEnum.JobClass]);
                              Assert.AreEqual("0", hash[JobStoreKeyEnum.IsDurable]);
                              Assert.AreEqual("0", hash[JobStoreKeyEnum.RequestRecovery]);
                              Assert.IsEmpty(hash[JobStoreKeyEnum.BlockedBy]);
                              Assert.IsEmpty(hash[JobStoreKeyEnum.BlockTime]);
                          });

            redis.When(x => x.HashSetAsync(schema.JobDataMapHashKey(job.Key), Arg.Any<HashEntry[]>()))
                 .Do(
                      info =>
                          {
                              var hash = info.Arg<HashEntry[]>().ToStringDictionary();
                              Assert.AreEqual(1, hash.Count);
                              Assert.AreEqual("test", hash["unit"]);
                          });
            redis.When(x => x.SetAddAsync(schema.JobsKey(), Arg.Any<RedisValue>()))
                 .Do(info => { Assert.AreEqual(schema.JobHashKey(job.Key), info.Arg<RedisValue>().ToString()); });
            redis.When(x => x.SetAddAsync(schema.JobGroupsKey(), Arg.Any<RedisValue>()))
                 .Do(info => { Assert.AreEqual(schema.JobGroupKey(job.Key.Group), info.Arg<RedisValue>().ToString()); });
            redis.When(x => x.SetAddAsync(schema.JobGroupKey(job.Key.Group), Arg.Any<RedisValue>()))
                 .Do(info => { Assert.AreEqual(schema.JobHashKey(job.Key), info.Arg<RedisValue>().ToString()); });

            Assert.DoesNotThrowAsync(async () => await storage.StoreJobAsync(job, false));
        }

        [Test]
        public async Task RemoveJobShouldBeSuccess()
        {
            var job = JobBuilder.Create<TestJob>().WithIdentity("Test", "Unit").WithDescription(description).UsingJobData("unit", "test").Build();

            redis.KeyDeleteAsync(schema.JobHashKey(job.Key)).Returns(true);
            redis.SetMembersAsync(schema.JobTriggersKey(job.Key))
                 .Returns(
                      new RedisValue[]
                          {
                              "prefix:Default:Trigger1"
                          });

            redis.SetLengthAsync(schema.JobGroupKey(job.Key.Group)).Returns(1);

            Assert.True(await storage.RemoveJobAsync(job.Key));

            RemoveJobAssert(job);
            redis.DidNotReceive().SetRemove(schema.JobGroupsKey(), job.Key.Group, CommandFlags.FireAndForget);
        }

        [Test]
        public async Task RemoveJobNotHaveTriggerShouldBeSuccess()
        {
            var job = JobBuilder.Create<TestJob>().WithIdentity("Test", "Unit").WithDescription(description).UsingJobData("unit", "test").Build();

            redis.KeyDeleteAsync(schema.JobHashKey(job.Key)).Returns(true);
            redis.SetMembersAsync(schema.JobTriggersKey(job.Key))
                 .Returns(new RedisValue[0]);
            redis.SetLengthAsync(schema.JobGroupKey(job.Key.Group)).Returns(0);

            Assert.True(await storage.RemoveJobAsync(job.Key));

            RemoveJobAssert(job);
            redis.Received(1).SetRemove(schema.JobGroupsKey(), schema.JobGroupKey(job.Key.Group), CommandFlags.FireAndForget);
        }

        private void RemoveJobAssert(IJobDetail job)
        {
            redis.Received(1).KeyDelete(schema.JobDataMapHashKey(job.Key), CommandFlags.FireAndForget);
            redis.Received(1).SetRemove(schema.JobsKey(), schema.JobHashKey(job.Key), CommandFlags.FireAndForget);
            redis.Received(1).SetRemove(schema.JobGroupKey(job.Key.Group), schema.JobHashKey(job.Key), CommandFlags.FireAndForget);
            redis.Received(1).KeyDelete(schema.JobTriggersKey(job.Key), CommandFlags.FireAndForget);
        }
    }
}