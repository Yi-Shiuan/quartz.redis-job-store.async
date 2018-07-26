namespace Quartz.RedisJobStore.Async
{
    #region

    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;

    using Common.Logging;

    using Newtonsoft.Json;
    using Newtonsoft.Json.Serialization;

    using Quartz.Impl.Matchers;
    using Quartz.RedisJobStore.Async.Enums;
    using Quartz.RedisJobStore.Async.Extensions;
    using Quartz.Spi;

    using StackExchange.Redis;

    #endregion

    #region

    #endregion

    public class RedisStorage
    {
        private readonly ILog logger;

        private readonly int misfireThreshold;

        private readonly IDatabase redis;

        private readonly string instanceId;

        private readonly ISchedulerSignaler signaler;

        private readonly RedisKeySchema schema;

        private readonly JsonSerializer serializer;

        public RedisStorage(RedisKeySchema redisJobStoreSchema, IDatabase db, ISchedulerSignaler signaler, string instanceId, int misfireThreshold)
        {
            schema = redisJobStoreSchema;
            redis = db;
            this.signaler = signaler;
            this.instanceId = instanceId;
            this.misfireThreshold = misfireThreshold;
            logger = LogManager.GetLogger<RedisStorage>();
            serializer = new JsonSerializer
                             {
                                 TypeNameHandling = TypeNameHandling.All,
                                 DateTimeZoneHandling = DateTimeZoneHandling.Utc,
                                 NullValueHandling = NullValueHandling.Ignore,
                                 ContractResolver = new CamelCasePropertyNamesContractResolver()
                             };
        }

        public Task<IReadOnlyCollection<IOperableTrigger>> AcquireNextTriggersAsync(DateTimeOffset noLaterThan, int maxCount, TimeSpan timeWindow)
        {
            throw new NotImplementedException();
            //var score = ToUnixTimeMilliseconds(noLaterThan.Add(timeWindow));
            //var acquireNextTriggers = redis.SortedSetRangeByScoreWithScoresAsync(
            //    schema.TriggerStateKey(Enums.TriggerState.Waiting),
            //    0,
            //    score,
            //    Exclude.None,
            //    Order.Ascending,
            //    0,
            //    maxCount);

            //if ((await acquireNextTriggers).Length == 0)
            //{
            //    return new List<IOperableTrigger>();
            //}

            //var triggers = new List<IOperableTrigger>((await acquireNextTriggers).Length);

            //foreach (var item in await acquireNextTriggers)
            //{
            //    var trigger = RetrieveTriggerAsync(schema.TriggerKey(item.Element));
            //    //await SetTriggerStateAsync(TriggerState.Acquired, item.Score, item.Element);
            //    triggers.Add(await trigger);
            //}

            //return triggers;
        }

        public Task<IReadOnlyCollection<string>> CalendarNamesAsync()
        {
            throw new NotImplementedException();
        }

        public Task<bool> CheckExistsAsync(JobKey key)
        {
            return redis.KeyExistsAsync(schema.RedisJobKey(key));
        }

        public Task<bool> CheckExistsAsync(string calName)
        {
            return redis.KeyExistsAsync(schema.RedisCalendarKey(calName));
        }

        public Task<bool> CheckExistsAsync(TriggerKey key)
        {
            return redis.KeyExistsAsync(schema.RedisTriggerKey(key));
        }

        public Task ClearAllSchedulingDataAsync()
        {
            throw new NotImplementedException();
        }

        public async Task<IReadOnlyCollection<JobKey>> GetJobKeysAsync(GroupMatcher<JobKey> matcher)
        {
            var allJobs = redis.SetMembersAsync(schema.RedisJobGroupKey());
            var result = new List<JobKey>();

            foreach (var item in await allJobs)
            {
                if (!item.IsNullOrEmpty && matcher.CompareWithOperator.Evaluate(item, matcher.CompareToValue))
                {
                    result.AddRange((await redis.SetMembersAsync(schema.RedisJobGroupKey(item))).Select(s => schema.ToJobKey(s)));
                }
            }

            return result;
        }

        public Task<IReadOnlyCollection<string>> GetPausedTriggerGroupsAsync()
        {
            throw new NotImplementedException();
        }

        public async Task<IReadOnlyCollection<TriggerKey>> GetTriggerKeysAsync(GroupMatcher<TriggerKey> matcher)
        {
            var triggerGroups = redis.SetMembersAsync(schema.RedisTriggerGroupKey());
            var result = new List<TriggerKey>();

            foreach (var item in await triggerGroups)
            {
                if (!item.IsNullOrEmpty && matcher.CompareWithOperator.Evaluate(item, matcher.CompareToValue))
                {
                    result.AddRange((await redis.SetMembersAsync(schema.RedisTriggerGroupKey(item))).Select(s => schema.ToTriggerKey(s)));
                }
            }

            return result;
        }

        public Task<IReadOnlyCollection<IOperableTrigger>> GetTriggersForJobAsync(JobKey key)
        {
            throw new NotImplementedException();
        }

        public Task<TriggerState> GetTriggerStateAsync(TriggerKey key)
        {
            throw new NotImplementedException();
        }

        public Task<bool> IsJobGroupPausedAsync(string name)
        {
            throw new NotImplementedException();
        }

        public Task<bool> IsTriggerGroupPausedAsync(string groupName)
        {
            throw new NotImplementedException();
        }

        public Task<IReadOnlyCollection<string>> JobGroupNamesAsync()
        {
            throw new NotImplementedException();
        }

        public Task<int> NumberOfCalendarsAsync()
        {
            throw new NotImplementedException();
        }

        public Task<int> NumberOfJobsAsync()
        {
            throw new NotImplementedException();
        }

        public Task<int> NumberOfTriggersAsync()
        {
            throw new NotImplementedException();
        }

        public Task PauseAllTriggersAsync()
        {
            throw new NotImplementedException();
        }

        public Task PauseJobAsync(JobKey jobKey)
        {
            throw new NotImplementedException();
        }

        public Task<IReadOnlyCollection<string>> PauseJobsAsync(GroupMatcher<JobKey> matcher)
        {
            throw new NotImplementedException();
        }

        public Task PauseTriggerAsync(TriggerKey triggerKey)
        {
            throw new NotImplementedException();
        }

        public Task<IReadOnlyCollection<string>> PauseTriggersAsync(GroupMatcher<TriggerKey> matcher)
        {
            throw new NotImplementedException();
        }

        public Task ReleaseAcquiredTriggerAsync(IOperableTrigger trigger)
        {
            throw new NotImplementedException();
        }

        public Task<bool> RemoveCalendarAsync(string name)
        {
            throw new NotImplementedException();
        }

        public Task<bool> RemoveJobAsync(JobKey jobKey)
        {
            throw new NotImplementedException();
        }

        public Task<bool> RemoveTriggerAsync(TriggerKey key, bool removeNonDurableJob = true)
        {
            throw new NotImplementedException();
        }

        public Task<bool> ReplaceTriggerAsync(TriggerKey key, IOperableTrigger newTrigger)
        {
            throw new NotImplementedException();
        }

        public Task ResumeAllTriggersAsync()
        {
            throw new NotImplementedException();
        }

        public Task ResumeJobAsync(JobKey jobKey)
        {
            throw new NotImplementedException();
        }

        public Task<IReadOnlyCollection<string>> ResumeJobsAsync(GroupMatcher<JobKey> matcher)
        {
            throw new NotImplementedException();
        }

        public Task ResumeTriggerAsync(TriggerKey triggerKey)
        {
            throw new NotImplementedException();
        }

        public Task<IReadOnlyCollection<string>> ResumeTriggersAsync(GroupMatcher<TriggerKey> matcher)
        {
            throw new NotImplementedException();
        }

        public Task<ICalendar> RetrieveCalendarAsync(string name)
        {
            throw new NotImplementedException();
        }

        public Task<IJobDetail> RetrieveJobAsync(JobKey key)
        {
            throw new NotImplementedException();
        }

        public Task<IOperableTrigger> RetrieveTriggerAsync(TriggerKey key)
        {
            throw new NotImplementedException();
        }

        public Task StoreCalendarAsync(string name, ICalendar calendar, bool replaceExisting, bool updateTriggers)
        {
            throw new NotImplementedException();
        }

        public async Task StoreJobAsync(IJobDetail job, bool replaceExisting)
        {
            var redisJobKey = schema.RedisJobKey(job.Key);

            if (await redis.KeyExistsAsync(redisJobKey) && !replaceExisting)
            {
                throw new ObjectAlreadyExistsException(job);
            }

            var jobStoreKey = schema.JobStoreKey(job.Key);

            redis.SetAdd(schema.RedisJobGroupKey(job.Key), jobStoreKey, CommandFlags.FireAndForget);
            redis.SetAdd(schema.RedisJobKey(), jobStoreKey, CommandFlags.FireAndForget);
            redis.SetAdd(schema.RedisJobGroupKey(), schema.JobGroupStoreKey(job.Key), CommandFlags.FireAndForget);
            redis.HashSet(schema.RedisJobDataMap(job.Key), job.JobDataMap.ToStoreEntity(), CommandFlags.FireAndForget);
            redis.HashSet(redisJobKey, job.ToStoreEntries(), CommandFlags.FireAndForget);
        }

        public async Task StoreTriggerAsync(ITrigger trigger, bool replaceExisting)
        {
            if (!(trigger is ISimpleTrigger || trigger is ICronTrigger))
            {
                throw new NotImplementedException("Only SimpleTrigger and CronTrigger are supported");
            }

            var redisTriggerKey = schema.RedisTriggerKey(trigger.Key);
            var isTriggerExisted = redis.KeyExistsAsync(redisTriggerKey);
            if (!replaceExisting && await isTriggerExisted)
            {
                throw new ObjectAlreadyExistsException(trigger);
            }

            var triggerStoreKey = schema.TriggerStoreKey(trigger.Key);
            redis.SetAdd(schema.RedisTriggerGroupKey(trigger.Key), triggerStoreKey, CommandFlags.FireAndForget);
            redis.SetAdd(schema.RedisTriggerKey(), triggerStoreKey, CommandFlags.FireAndForget);
            redis.SetAdd(schema.RedisTriggerGroupKey(), schema.TriggerGroupStoreKey(trigger.Key), CommandFlags.FireAndForget);
            redis.HashSet(redisTriggerKey, trigger.ToStoreEntries(schema.JobStoreKey(trigger.JobKey)), CommandFlags.FireAndForget);
            redis.SetAdd(schema.RedisTriggerJobKey(trigger.JobKey), triggerStoreKey, CommandFlags.FireAndForget);

            if (!string.IsNullOrEmpty(trigger.CalendarName))
            {
                redis.SetAdd(schema.RedisCalendarKey(trigger.CalendarName), triggerStoreKey, CommandFlags.FireAndForget);
            }

            if (!await isTriggerExisted)
            {
                return;
            }
            
            foreach (TriggerRedisState item in Enum.GetValues(typeof(TriggerRedisState)))
            {
                await redis.SortedSetRemoveAsync(schema.RedisTriggerStateKey(item), triggerStoreKey);
            }
        }

        public Task TriggeredJobCompleteAsync(IOperableTrigger trigger, IJobDetail jobDetail, SchedulerInstruction triggerInstCode)
        {
            throw new NotImplementedException();
        }

        public Task<IReadOnlyCollection<string>> TriggerGroupNamesAsync()
        {
            throw new NotImplementedException();
        }

        public Task<IReadOnlyCollection<TriggerFiredResult>> TriggersFiredAsync(IEnumerable<IOperableTrigger> triggers)
        {
            throw new NotImplementedException();
        }

        public Task<bool> UnsetTriggerStateAsync(string triggerHashKey)
        {
            throw new NotImplementedException();
        }

        protected Task<bool> ApplyMisfireAsync(IOperableTrigger trigger)
        {
            throw new NotImplementedException();
        }

        protected IDictionary<string, string> ConvertToDictionaryString(HashEntry[] entries)
        {
            throw new NotImplementedException();
        }
    }
}