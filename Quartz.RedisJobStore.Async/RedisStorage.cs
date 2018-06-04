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

namespace Quartz.RedisJobStore.Async
{
    #region

    #endregion

    public class RedisStorage
    {
        private readonly ILog logger;

        private readonly int misfireThreshold;

        private readonly IDatabase redis;

        private readonly string schedulerInstanceId;

        private readonly ISchedulerSignaler schedulerSignaler;

        private readonly RedisKeySchema schema;

        private readonly JsonSerializer serializer;

        private readonly DateTime unixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        public RedisStorage(RedisKeySchema redisJobStoreSchema, IDatabase db, ISchedulerSignaler signaler, string schedulerInstanceId, int misfireThreshold)
        {
            schema = redisJobStoreSchema;
            redis = db;
            schedulerSignaler = signaler;
            this.schedulerInstanceId = schedulerInstanceId;
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

        public async Task<IReadOnlyCollection<IOperableTrigger>> AcquireNextTriggersAsync(DateTimeOffset noLaterThan, int maxCount, TimeSpan timeWindow)
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

        public async Task<IReadOnlyCollection<string>> CalendarNamesAsync()
        {
            throw new NotImplementedException();
        }

        public Task<bool> CheckExistsAsync(JobKey jobKey)
        {
            throw new NotImplementedException();
        }

        public Task<bool> CheckExistsAsync(string calName)
        {
            throw new NotImplementedException();
        }

        public Task<bool> CheckExistsAsync(TriggerKey triggerKey)
        {
            throw new NotImplementedException();
        }

        public async Task ClearAllSchedulingDataAsync()
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
                    var groupJobs = redis.SetMembersAsync(schema.RedisJobGroupKey(item));

                    result.AddRange((await groupJobs).Select(s => schema.ToJobKey(s)));
                }
            }

            return result;
        }

        public async Task<IReadOnlyCollection<string>> GetPausedTriggerGroupsAsync()
        {
            throw new NotImplementedException();
        }

        public async Task<IReadOnlyCollection<IOperableTrigger>> GetTriggersForJobAsync(JobKey jobKey)
        {
            throw new NotImplementedException();
        }

        public async Task<TriggerState> GetTriggerStateAsync(TriggerKey triggerKey)
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

        public async Task<IReadOnlyCollection<string>> JobGroupNamesAsync()
        {
            throw new NotImplementedException();
        }

        public async Task<int> NumberOfCalendarsAsync()
        {
            throw new NotImplementedException();
        }

        public async Task<int> NumberOfJobsAsync()
        {
            throw new NotImplementedException();
        }

        public async Task<int> NumberOfTriggersAsync()
        {
            throw new NotImplementedException();
        }

        public async Task PauseAllTriggersAsync()
        {
            throw new NotImplementedException();
        }

        public async Task PauseJobAsync(JobKey jobKey)
        {
            throw new NotImplementedException();
        }

        public async Task<IReadOnlyCollection<string>> PauseJobsAsync(GroupMatcher<JobKey> matcher)
        {
            throw new NotImplementedException();
        }

        public async Task PauseTriggerAsync(TriggerKey triggerKey)
        {
            throw new NotImplementedException();
        }

        public async Task<IReadOnlyCollection<string>> PauseTriggersAsync(GroupMatcher<TriggerKey> matcher)
        {
            throw new NotImplementedException();
        }

        public async Task ReleaseAcquiredTriggerAsync(IOperableTrigger trigger)
        {
            throw new NotImplementedException();
        }

        public async Task<bool> RemoveCalendarAsync(string name)
        {
            throw new NotImplementedException();
        }

        public async Task<bool> RemoveJobAsync(JobKey jobKey)
        {
            throw new NotImplementedException();
        }

        public async Task<bool> RemoveTriggerAsync(TriggerKey triggerKey, bool removeNonDurableJob = true)
        {
            throw new NotImplementedException();
        }

        public async Task<bool> ReplaceTriggerAsync(TriggerKey triggerKey, IOperableTrigger newTrigger)
        {
            throw new NotImplementedException();
        }

        public async Task ResumeAllTriggersAsync()
        {
            throw new NotImplementedException();
        }

        public async Task ResumeJobAsync(JobKey jobKey)
        {
            throw new NotImplementedException();
        }

        public async Task<IReadOnlyCollection<string>> ResumeJobsAsync(GroupMatcher<JobKey> matcher)
        {
            throw new NotImplementedException();
        }

        public async Task ResumeTriggerAsync(TriggerKey triggerKey)
        {
            throw new NotImplementedException();
        }

        public async Task<IReadOnlyCollection<string>> ResumeTriggersAsync(GroupMatcher<TriggerKey> matcher)
        {
            throw new NotImplementedException();
        }

        public async Task<ICalendar> RetrieveCalendarAsync(string name)
        {
            throw new NotImplementedException();
        }

        public async Task<IJobDetail> RetrieveJobAsync(JobKey jobKey)
        {
            throw new NotImplementedException();
        }

        public async Task<IOperableTrigger> RetrieveTriggerAsync(TriggerKey triggerKey)
        {
            throw new NotImplementedException();
        }

        public async Task StoreCalendarAsync(string name, ICalendar calendar, bool replaceExisting, bool updateTriggers)
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

            if (await isTriggerExisted)
            {
                foreach (TriggerRedisState item in Enum.GetValues(typeof(TriggerRedisState)))
                {
                    await redis.SortedSetRemoveAsync(schema.RedisTriggerStateKey(item), triggerStoreKey);
                }
            }
        }

        public async Task TriggeredJobCompleteAsync(IOperableTrigger trigger, IJobDetail jobDetail, SchedulerInstruction triggerInstCode)
        {
            throw new NotImplementedException();
        }

        public async Task<IReadOnlyCollection<string>> TriggerGroupNamesAsync()
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
                    var groupJobs = redis.SetMembersAsync(schema.RedisTriggerGroupKey(item));

                    result.AddRange((await groupJobs).Select(s => schema.ToTriggerKey(s)));
                }
            }

            return result;
        }

        public async Task<IReadOnlyCollection<TriggerFiredResult>> TriggersFiredAsync(IEnumerable<IOperableTrigger> triggers)
        {
            throw new NotImplementedException();
        }

        public async Task<bool> UnsetTriggerStateAsync(string triggerHashKey)
        {
            throw new NotImplementedException();
        }

        protected async Task<bool> ApplyMisfireAsync(IOperableTrigger trigger)
        {
            throw new NotImplementedException();
        }

        protected IDictionary<string, string> ConvertToDictionaryString(HashEntry[] entries)
        {
            throw new NotImplementedException();
        }

        //protected HashEntry[] ConvertToHashEntries(ITrigger trigger)
        //{
        //    var operable = (IOperableTrigger)trigger;
        //    if (operable == null)
        //    {
        //        throw new InvalidCastException("trigger needs to be IOperable");
        //    }

        //    var entries = new List<HashEntry>
        //                      {
        //                          new HashEntry(
        //                              TriggerStoreKey.JobHash,
        //                              operable.JobKey == null ? string.Empty : schema.JobHashKey(operable.JobKey)),
        //                          new HashEntry(TriggerStoreKey.Description, operable.Description ?? string.Empty),
        //                          new HashEntry(
        //                              TriggerStoreKey.NextFireTime,
        //                              operable.GetNextFireTimeUtc().HasValue
        //                                  ? operable.GetNextFireTimeUtc().GetValueOrDefault().DateTime.ToUnixTimeMillieSeconds().ToString(CultureInfo.InvariantCulture)
        //                                  : string.Empty),
        //                          new HashEntry(
        //                              TriggerStoreKey.PrevFireTime,
        //                              operable.GetPreviousFireTimeUtc().HasValue
        //                                  ? operable.GetPreviousFireTimeUtc().GetValueOrDefault().DateTime.ToUnixTimeMillieSeconds().ToString(CultureInfo.InvariantCulture)
        //                                  : string.Empty),
        //                          new HashEntry(TriggerStoreKey.Priority, operable.Priority),
        //                          new HashEntry(
        //                              TriggerStoreKey.StartTime,
        //                              operable.StartTimeUtc.DateTime.ToUnixTimeMillieSeconds()),
        //                          new HashEntry(
        //                              TriggerStoreKey.EndTime,
        //                              operable.EndTimeUtc.HasValue
        //                                  ? operable.EndTimeUtc.Value.DateTime.ToUnixTimeMillieSeconds().ToString(CultureInfo.InvariantCulture)
        //                                  : string.Empty),
        //                          new HashEntry(
        //                              TriggerStoreKey.FinalFireTime,
        //                              operable.FinalFireTimeUtc.HasValue
        //                                  ? operable.FinalFireTimeUtc.Value.DateTime.ToUnixTimeMillieSeconds().ToString(CultureInfo.InvariantCulture)
        //                                  : string.Empty),
        //                          new HashEntry(TriggerStoreKey.FireInstanceId, operable.FireInstanceId ?? string.Empty),
        //                          new HashEntry(TriggerStoreKey.MisfireInstruction, operable.MisfireInstruction),
        //                          new HashEntry(TriggerStoreKey.CalendarName, operable.CalendarName ?? string.Empty)
        //                      };

        //    switch (operable)
        //    {
        //        case ISimpleTrigger _:
        //            entries.Add(new HashEntry(TriggerStoreKey.TriggerType, TriggerStoreKey.TriggerTypeSimple));
        //            entries.Add(new HashEntry(TriggerStoreKey.RepeatCount, ((ISimpleTrigger)operable).RepeatCount));
        //            entries.Add(new HashEntry(TriggerStoreKey.RepeatInterval, ((ISimpleTrigger)operable).RepeatInterval.ToString()));
        //            entries.Add(new HashEntry(TriggerStoreKey.TimesTriggered, ((ISimpleTrigger)operable).TimesTriggered));
        //            break;
        //        case ICronTrigger _:
        //            entries.Add(new HashEntry(TriggerStoreKey.TriggerType, TriggerStoreKey.TriggerTypeCron));
        //            entries.Add(new HashEntry(TriggerStoreKey.CronExpression, ((ICronTrigger)operable).CronExpressionString));
        //            entries.Add(new HashEntry(TriggerStoreKey.TimeZoneId, ((ICronTrigger)operable).TimeZone.Id));
        //            break;
        //    }

        //    return entries.ToArray();
        //}

        //protected async Task<double> GetLastTriggersReleaseTimeAsync()
        //{
        //    var lastReleaseTime = redis.StringGetAsync(schema.LastTriggerReleaseTime());

        //    return string.IsNullOrEmpty(await lastReleaseTime) ? 0 : double.Parse(await lastReleaseTime);
        //}

        //protected async Task ReleaseOrphanedTriggersAsync(TriggerState currentState, TriggerState newState)
        //{
        //    var triggers = redis.SortedSetRangeByScoreWithScoresAsync(schema.TriggerStateKey(currentState), 0, -1);

        //    foreach (var sortedSetEntry in await triggers)
        //    {
        //        var lockedId = redis.StringGetAsync(schema.TriggerLockKey(schema.TriggerKey(sortedSetEntry.Element.ToString())));
        //        if (string.IsNullOrEmpty(await lockedId))
        //        {
        //            await SetTriggerStateAsync(newState, sortedSetEntry.Score, sortedSetEntry.Element);
        //        }
        //    }
        //}

        //protected Task SetLastTriggerReleaseTimeAsync(double time)
        //{
        //    return redis.StringSetAsync(schema.LastTriggerReleaseTime(), time);
        //}

        //protected async Task<bool> SetTriggerStateAsync(TriggerState state, double score, string triggerHashKey)
        //{
        //    await UnsetTriggerStateAsync(triggerHashKey);
        //    return await redis.SortedSetAddAsync(schema.TriggerStateKey(state), triggerHashKey, score);
        //}

        //private DateTime DateTimeFromUnixTimestampMillis(double millis)
        //{
        //    return unixEpoch.AddMilliseconds(millis);
        //}

        //private object JsonDeSerialize(string jsonString)
        //{
        //    object result;

        //    using (var ms = new MemoryStream(Encoding.UTF8.GetBytes(jsonString)))
        //    using (var sr = new StreamReader(ms))
        //    using (var reader = new JsonTextReader(sr))
        //    {
        //        result = serializer.Deserialize(reader);
        //    }

        //    return result;
        //}

        //private string JsonSerialize<T>(T obj)
        //{
        //    string result;

        //    using (var ms = new MemoryStream())
        //    using (var sw = new StreamWriter(ms))
        //    using (var jsonTextWriter = new JsonTextWriter(sw))
        //    using (var reader = new StreamReader(ms, Encoding.UTF8))
        //    {
        //        serializer.Serialize(jsonTextWriter, obj);
        //        jsonTextWriter.Flush();
        //        ms.Seek(0, SeekOrigin.Begin);
        //        result = reader.ReadToEnd();
        //    }

        //    return result;
        //}

        //private void PopulateTrigger(TriggerKey triggerKey, IDictionary<string, string> properties, IOperableTrigger trigger)
        //{
        //    trigger.Key = triggerKey;
        //    trigger.JobKey = schema.JobKey(properties[TriggerStoreKey.JobHash]);
        //    trigger.Description = properties[TriggerStoreKey.Description];
        //    trigger.FireInstanceId = properties[TriggerStoreKey.FireInstanceId];
        //    trigger.CalendarName = properties[TriggerStoreKey.CalendarName];
        //    trigger.Priority = int.Parse(properties[TriggerStoreKey.Priority]);
        //    trigger.MisfireInstruction = int.Parse(properties[TriggerStoreKey.MisfireInstruction]);
        //    trigger.StartTimeUtc = DateTimeFromUnixTimestampMillis(double.Parse(properties[TriggerStoreKey.StartTime]));

        //    trigger.EndTimeUtc = string.IsNullOrEmpty(properties[TriggerStoreKey.EndTime])
        //                             ? default(DateTimeOffset?)
        //                             : DateTimeFromUnixTimestampMillis(double.Parse(properties[TriggerStoreKey.EndTime]));

        //    if (trigger is AbstractTrigger)
        //    {
        //        trigger.SetNextFireTimeUtc(
        //            string.IsNullOrEmpty(properties[TriggerStoreKey.NextFireTime])
        //                ? default(DateTimeOffset?)
        //                : DateTimeFromUnixTimestampMillis(double.Parse(properties[TriggerStoreKey.NextFireTime])));

        //        trigger.SetPreviousFireTimeUtc(
        //            string.IsNullOrEmpty(properties[TriggerStoreKey.PrevFireTime])
        //                ? default(DateTimeOffset?)
        //                : DateTimeFromUnixTimestampMillis(double.Parse(properties[TriggerStoreKey.PrevFireTime])));
        //    }
        //}

        //private IOperableTrigger RetrieveTrigger(TriggerKey triggerKey, IDictionary<string, string> properties)
        //{
        //    var type = properties[TriggerStoreKey.TriggerType];

        //    if (string.IsNullOrEmpty(type))
        //    {
        //        return null;
        //    }

        //    if (type.Equals(TriggerStoreKey.TriggerTypeSimple, StringComparison.OrdinalIgnoreCase))
        //    {
        //        var simpleTrigger = new SimpleTriggerImpl();

        //        if (!string.IsNullOrEmpty(properties[TriggerStoreKey.RepeatCount]))
        //        {
        //            simpleTrigger.RepeatCount = Convert.ToInt32(properties[TriggerStoreKey.RepeatCount]);
        //        }

        //        if (!string.IsNullOrEmpty(properties[TriggerStoreKey.RepeatInterval]))
        //        {
        //            simpleTrigger.RepeatInterval = TimeSpan.Parse(properties[TriggerStoreKey.RepeatInterval]);
        //        }

        //        if (!string.IsNullOrEmpty(properties[TriggerStoreKey.TimesTriggered]))
        //        {
        //            simpleTrigger.TimesTriggered = Convert.ToInt32(properties[TriggerStoreKey.TimesTriggered]);
        //        }

        //        PopulateTrigger(triggerKey, properties, simpleTrigger);

        //        return simpleTrigger;
        //    }

        //    var cronTrigger = new CronTriggerImpl();

        //    if (!string.IsNullOrEmpty(properties[TriggerStoreKey.TimeZoneId]))
        //    {
        //        cronTrigger.TimeZone = TimeZoneInfo.FindSystemTimeZoneById(properties[TriggerStoreKey.TimeZoneId]);
        //    }

        //    if (!string.IsNullOrEmpty(properties[TriggerStoreKey.CronExpression]))
        //    {
        //        cronTrigger.CronExpressionString = properties[TriggerStoreKey.CronExpression];
        //    }

        //    PopulateTrigger(triggerKey, properties, cronTrigger);

        //    return cronTrigger;
        //}

        //private async Task UpdateTriggerStateAsync(ITrigger trigger)
        //{
        //    var triggerPausedResult = redis.SetContainsAsync(schema.PausedTriggerGroupsKey(), schema.TriggerGroupSetKey(trigger.Key.Group));
        //    var jobPausedResult = redis.SetContainsAsync(schema.PausedJobGroupsKey(), schema.JobGroupKey(trigger.JobKey.Group));
        //    var nextFireTime = trigger.GetNextFireTimeUtc().HasValue
        //                           ? trigger.GetNextFireTimeUtc().GetValueOrDefault().DateTime.ToUnixTimeMillieSeconds()
        //                           : -1;
        //    if (await triggerPausedResult || await jobPausedResult)
        //    {
        //        var jobHashKey = schema.JobHashKey(trigger.JobKey);

        //        if (await redis.SetContainsAsync(schema.BlockedJobsSet(), jobHashKey))
        //        {
        //            await SetTriggerStateAsync(TriggerState.PausedBlocked, nextFireTime, schema.TriggerHashKey(trigger.Key));
        //        }
        //        else
        //        {
        //            await SetTriggerStateAsync(TriggerState.Paused, nextFireTime, schema.TriggerHashKey(trigger.Key));
        //        }
        //    }
        //    else if (trigger.GetNextFireTimeUtc().HasValue)
        //    {
        //        await SetTriggerStateAsync(TriggerState.Waiting, nextFireTime, schema.TriggerHashKey(trigger.Key));
        //    }
        //}
    }
}