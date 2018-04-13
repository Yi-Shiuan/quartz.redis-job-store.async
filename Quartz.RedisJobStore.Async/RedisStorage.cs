﻿namespace Quartz.RedisJobStore.Async
{
    #region

    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    using Common.Logging;

    using Newtonsoft.Json;
    using Newtonsoft.Json.Serialization;

    using Quartz.Impl.Matchers;
    using Quartz.Impl.Triggers;
    using Quartz.RedisJobStore.Async.Enums;
    using Quartz.Spi;

    using StackExchange.Redis;

    #endregion

    public class RedisStorage
    {
        private readonly ILog logger;

        private readonly int misfireThreshold;

        private readonly IDatabase redis;

        private readonly int redisLockTimeout;

        private readonly string schedulerInstanceId;

        private readonly ISchedulerSignaler schedulerSignaler;

        private readonly RedisKeySchema schema;

        private readonly JsonSerializer serializer;

        private readonly int triggerLockTimeout;

        private readonly DateTime unixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        private string lockValue;

        public RedisStorage(
            RedisKeySchema redisJobStoreSchema,
            IDatabase db,
            ISchedulerSignaler signaler,
            string schedulerInstanceId,
            int triggerLockTimeout,
            int redisLockTimeout,
            int misfireThreshold)
        {
            schema = redisJobStoreSchema;
            redis = db;
            schedulerSignaler = signaler;
            this.schedulerInstanceId = schedulerInstanceId;
            this.triggerLockTimeout = triggerLockTimeout;
            this.redisLockTimeout = redisLockTimeout;
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

        public async Task<IReadOnlyCollection<IOperableTrigger>> AcquireNextTriggersAsync(
            DateTimeOffset noLaterThan,
            int maxCount,
            TimeSpan timeWindow)
        {
            await ReleaseTriggersAsync();

            var triggers = new List<IOperableTrigger>();

            var retry = false;

            do
            {
                var acquiredJobHashKeysForNoConcurrentExec = new HashSet<string>();

                var score = ToUnixTimeMilliseconds(noLaterThan.Add(timeWindow));

                var waitingStateTriggers = redis.SortedSetRangeByScoreWithScoresAsync(
                                               schema.TriggerStateKey(TriggerStateEnum.Waiting),
                                               0,
                                               score,
                                               Exclude.None,
                                               Order.Ascending,
                                               0,
                                               maxCount);
                foreach (var sortedSetEntry in await waitingStateTriggers)
                {
                    var trigger = RetrieveTriggerAsync(schema.TriggerKey(sortedSetEntry.Element));

                    if (await ApplyMisfireAsync(await trigger))
                    {
                        retry = true;
                        break;
                    }

                    if (!(await trigger).GetNextFireTimeUtc().HasValue)
                    {
                        await UnsetTriggerStateAsync(sortedSetEntry.Element);
                        continue;
                    }

                    var jobHashKey = schema.JobHashKey((await trigger).JobKey);

                    var job = RetrieveJobAsync((await trigger).JobKey);

                    if ((await job) != null && (await job).ConcurrentExecutionDisallowed)
                    {
                        if (acquiredJobHashKeysForNoConcurrentExec.Contains(jobHashKey))
                        {
                            continue;
                        }

                        acquiredJobHashKeysForNoConcurrentExec.Add(jobHashKey);
                    }

                    await LockTriggerAsync((await trigger).Key);
                    await SetTriggerStateAsync(TriggerStateEnum.Acquired, sortedSetEntry.Score, sortedSetEntry.Element);
                    triggers.Add(await trigger);
                }
            }
            while (retry);

            return triggers;
        }

        public async Task<IReadOnlyCollection<string>> CalendarNamesAsync()
        {
            var calendarsSet = redis.SetMembersAsync(schema.CalendarsKey());

            var result = new List<string>((await calendarsSet).Length);

            foreach (var value in await calendarsSet)
            {
                result.Add(schema.GetCalendarName(value));
            }

            return result;
        }

        public Task<bool> CheckExistsAsync(JobKey jobKey)
        {
            return redis.KeyExistsAsync(schema.JobHashKey(jobKey));
        }

        public Task<bool> CheckExistsAsync(string calName)
        {
            return redis.KeyExistsAsync(schema.CalendarHashKey(calName));
        }

        public Task<bool> CheckExistsAsync(TriggerKey triggerKey)
        {
            return redis.KeyExistsAsync(schema.TriggerHashKey(triggerKey));
        }

        public async Task ClearAllSchedulingDataAsync()
        {
            // delete triggers
            foreach (string jobHashKey in await redis.SetMembersAsync(schema.JobsKey()))
            {
                await RemoveJobAsync(schema.JobKey(jobHashKey));
            }

            foreach (var triggerHashKey in await redis.SetMembersAsync(schema.TriggersKey()))
            {
                await RemoveTriggerAsync(schema.TriggerKey(triggerHashKey));
            }

            foreach (var calHashName in await redis.SetMembersAsync(schema.CalendarsKey()))
            {
                redis.KeyDelete(schema.CalendarTriggersKey(schema.GetCalendarName(calHashName)), CommandFlags.FireAndForget);
                await RemoveCalendarAsync(schema.GetCalendarName(calHashName));
            }

            redis.KeyDelete(schema.PausedTriggerGroupsKey(), CommandFlags.FireAndForget);
            redis.KeyDelete(schema.PausedJobGroupsKey(), CommandFlags.FireAndForget);
        }

        public async Task<IReadOnlyCollection<string>> GetPausedTriggerGroupsAsync()
        {
            var triggerGroupSetKeys = redis.SetMembersAsync(schema.PausedTriggerGroupsKey());

            var groups = new List<string>((await triggerGroupSetKeys).Length);

            foreach (var triggerGroupSetKey in await triggerGroupSetKeys)
            {
                groups.Add(schema.TriggerGroup(triggerGroupSetKey));
            }

            return groups;
        }

        public async Task<IReadOnlyCollection<IOperableTrigger>> GetTriggersForJobAsync(JobKey jobKey)
        {
            var jobTrigger = schema.JobTriggersKey(jobKey);
            var triggerKeys = redis.SetMembersAsync(jobTrigger);
            
            return await Task.WhenAll((await triggerKeys).Select(s => RetrieveTriggerAsync(schema.TriggerKey(s))));
        }

        public async Task<TriggerState> GetTriggerStateAsync(TriggerKey triggerKey)
        {
            var key = schema.TriggerHashKey(triggerKey);

            if (await redis.SortedSetScoreAsync(schema.TriggerStateKey(TriggerStateEnum.Paused), key) != null
                || await redis.SortedSetScoreAsync(schema.TriggerStateKey(TriggerStateEnum.PausedBlocked), key) != null)
            {
                return TriggerState.Paused;
            }

            if (await redis.SortedSetScoreAsync(schema.TriggerStateKey(TriggerStateEnum.Blocked), key) != null)
            {
                return TriggerState.Blocked;
            }

            if (await redis.SortedSetScoreAsync(schema.TriggerStateKey(TriggerStateEnum.Waiting), key) != null
                || await redis.SortedSetScoreAsync(schema.TriggerStateKey(TriggerStateEnum.Acquired), key) != null)
            {
                return TriggerState.Normal;
            }

            if (await redis.SortedSetScoreAsync(schema.TriggerStateKey(TriggerStateEnum.Completed), key) != null)
            {
                return TriggerState.Complete;
            }

            if (await redis.SortedSetScoreAsync(schema.TriggerStateKey(TriggerStateEnum.Error), key) != null)
            {
                return TriggerState.Error;
            }

            return TriggerState.None;
        }

        public Task<bool> IsJobGroupPausedAsync(string name)
        {
            return redis.SetContainsAsync(schema.PausedJobGroupsKey(), schema.JobGroupKey(name));
        }

        public Task<bool> IsTriggerGroupPausedAsync(string groupName)
        {
            return redis.SetContainsAsync(schema.PausedTriggerGroupsKey(), schema.TriggerGroupSetKey(groupName));
        }

        public async Task<IReadOnlyCollection<string>> JobGroupNamesAsync()
        {
            var groupsSet = redis.SetMembersAsync(schema.JobGroupsKey());
            var result = new List<string>((await groupsSet).Length);

            foreach (var value in await groupsSet)
            {
                result.Add(schema.JobGroup(value));
            }

            return result;
        }

        public async Task<IReadOnlyCollection<JobKey>> JobKeysAsync(GroupMatcher<JobKey> matcher)
        {
            var jobKeys = new HashSet<JobKey>();

            if (matcher.CompareWithOperator.Equals(StringOperator.Equality))
            {
                var jobGroupSetKey = schema.JobGroupKey(matcher.CompareToValue);
                var jobHashKeys = await redis.SetMembersAsync(jobGroupSetKey);
                if (jobHashKeys != null)
                {
                    foreach (var jobHashKey in jobHashKeys)
                    {
                        jobKeys.Add(schema.JobKey(jobHashKey));
                    }
                }
            }
            else
            {
                var jobGroupSets = await redis.SetMembersAsync(schema.JobGroupsKey());

                foreach (var item in jobGroupSets)
                {
                    if (matcher.CompareWithOperator.Evaluate(schema.JobGroup(item), matcher.CompareToValue))
                    {
                        var keys = await redis.SetMembersAsync(item.ToString());

                        if (keys != null)
                        {
                            jobKeys.Add(schema.JobKey(item));
                        }
                    }
                }
            }

            return jobKeys;
        }

        public async Task LockWithWaitAsync()
        {
            while (!await LockAsync())
            {
                try
                {
                    logger.Info("waiting for redis lock");
                    Thread.Sleep(RandomInt(75, 125));
                }
                catch (ThreadInterruptedException ex)
                {
                    logger.ErrorFormat("error out on waiting for a lock", ex);
                }
            }
        }

        public async Task<int> NumberOfCalendarsAsync()
        {
            return (int)await redis.SetLengthAsync(schema.CalendarsKey());
        }

        public async Task<int> NumberOfJobsAsync()
        {
            return (int)await redis.SetLengthAsync(schema.JobsKey());
        }

        public async Task<int> NumberOfTriggersAsync()
        {
            return (int)await redis.SetLengthAsync(schema.TriggersKey());
        }

        public async Task PauseAllTriggersAsync()
        {
            var triggerGroups = await redis.SetMembersAsync(schema.TriggerGroupsKey());
            await Task.WhenAll(triggerGroups.Select(group => PauseTriggersAsync(GroupMatcher<TriggerKey>.GroupEquals(schema.TriggerGroup(group)))));
        }

        public async Task PauseJobAsync(JobKey jobKey)
        {
            var triggers = GetTriggersForJobAsync(jobKey);
            await Task.WhenAll((await triggers).Select(s => PauseTriggerAsync(s.Key)));
        }

        public async Task<IReadOnlyCollection<string>> PauseJobsAsync(GroupMatcher<JobKey> matcher)
        {
            var jobGroupSets = redis.SetMembersAsync(schema.JobGroupsKey());

            var groups = (await jobGroupSets)
                           .Where(x => matcher.CompareWithOperator.Evaluate(schema.JobGroup(x), matcher.CompareToValue))
                           .ToDictionary(d => d, v => redis.SetMembersAsync(v.ToString()));

            var pausedJobGroups = new List<string>(groups.Count);

            foreach (var group in groups)
            {
                if (!await redis.SetAddAsync(schema.PausedJobGroupsKey(), @group.Key))
                {
                    continue;
                }

                await Task.WhenAll((await @group.Value).Select(s => PauseJobAsync(schema.JobKey(s))));
                pausedJobGroups.Add(schema.JobGroup(@group.Key));
            }

            return pausedJobGroups;
        }

        public async Task PauseTriggerAsync(TriggerKey triggerKey)
        {
            var triggerHashKey = schema.TriggerHashKey(triggerKey);
            var isTriggerExist = redis.KeyExistsAsync(triggerHashKey);
            var isCompleted = redis.SortedSetScoreAsync(schema.TriggerStateKey(TriggerStateEnum.Completed), triggerHashKey);
            var nextFireTimeResult = redis.HashGetAsync(triggerHashKey, TriggerStoreKeyEnum.NextFireTime);
            var blockedScoreResult = redis.SortedSetScoreAsync(schema.TriggerStateKey(TriggerStateEnum.Blocked), triggerHashKey);

            if (!await isTriggerExist)
            {
                return;
            }

            if ((await isCompleted).HasValue)
            {
                return;
            }

            double.TryParse(string.IsNullOrEmpty(await nextFireTimeResult) ? "-1" : (await nextFireTimeResult).ToString(), out var nextFireTime);

            if ((await blockedScoreResult).HasValue)
            {
                await SetTriggerStateAsync(TriggerStateEnum.PausedBlocked, nextFireTime, triggerHashKey);
                return;
            }

            await SetTriggerStateAsync(TriggerStateEnum.Paused, nextFireTime, triggerHashKey);
        }

        public async Task<IReadOnlyCollection<string>> PauseTriggersAsync(GroupMatcher<TriggerKey> matcher)
        {
            var allTriggerGroups = redis.SetMembersAsync(schema.TriggerGroupsKey());

            var result = new Dictionary<string, Task<RedisValue[]>>((await allTriggerGroups).Length);
            var pausedTriggerGroups = new List<string>((await allTriggerGroups).Length);

            foreach (var trigger in await allTriggerGroups)
            {
                if (matcher.CompareWithOperator.Evaluate(schema.TriggerGroup(trigger), matcher.CompareToValue))
                {
                    result[trigger.ToString()] = redis.SetMembersAsync(trigger.ToString());
                }
            }
            
            foreach (var triggerGroup in result)
            {
                if (!await redis.SetAddAsync(schema.PausedTriggerGroupsKey(), triggerGroup.Key))
                {
                    continue;
                }

                await Task.WhenAll((await triggerGroup.Value).Select(s => PauseTriggerAsync(schema.TriggerKey(s))));
                pausedTriggerGroups.Add(schema.TriggerGroup(triggerGroup.Key));
            }

            return pausedTriggerGroups;
        }

        public async Task ReleaseAcquiredTriggerAsync(IOperableTrigger trigger)
        {
            var triggerHashKey = schema.TriggerHashKey(trigger.Key);

            var score = redis.SortedSetScoreAsync(schema.TriggerStateKey(TriggerStateEnum.Acquired), triggerHashKey);

            if (!(await score).HasValue)
            {
                return;
            }

            if (trigger.GetNextFireTimeUtc().HasValue)
            {
                await SetTriggerStateAsync(
                    TriggerStateEnum.Waiting,
                    trigger.GetNextFireTimeUtc().GetValueOrDefault().DateTime.ToUnixTimeMillieSeconds(),
                    triggerHashKey);
                return;
            }

            await UnsetTriggerStateAsync(triggerHashKey);
        }

        public async Task<bool> RemoveCalendarAsync(string name)
        {
            if (await redis.SetLengthAsync(schema.CalendarTriggersKey(name)) > 0)
            {
                throw new JobPersistenceException($"There are triggers are using calendar {name}");
            }

            var hashKey = schema.CalendarHashKey(name);

            return await redis.KeyDeleteAsync(hashKey) & await redis.SetRemoveAsync(schema.CalendarsKey(), hashKey);
        }

        public async Task<bool> RemoveJobAsync(JobKey jobKey)
        {
            var hashKey = schema.JobHashKey(jobKey);
            var groupKey = schema.JobGroupKey(jobKey.Group);
            var triggersKey = schema.JobTriggersKey(jobKey);

            var result = redis.KeyDeleteAsync(hashKey);
            redis.KeyDelete(schema.JobDataMapHashKey(jobKey), CommandFlags.FireAndForget);
            redis.SetRemove(schema.JobsKey(), hashKey, CommandFlags.FireAndForget);
            redis.SetRemove(groupKey, hashKey, CommandFlags.FireAndForget);

            var triggers = await redis.SetMembersAsync(triggersKey);

            redis.KeyDelete(triggersKey, CommandFlags.FireAndForget);

            var length = redis.SetLengthAsync(groupKey);

            if (await length == 0)
            {
                redis.SetRemove(schema.JobGroupsKey(), groupKey, CommandFlags.FireAndForget);
            }

            var tasks = new List<Task>(triggers.Length);

            foreach (var trigger in triggers)
            {
                var triggerKey = schema.TriggerKey(trigger);
                var triggerGroupKey = schema.TriggerGroupSetKey(triggerKey.Group);

                tasks.Add(UnsetTriggerStateAsync(trigger));
                redis.SetRemove(schema.TriggersKey(), trigger, CommandFlags.FireAndForget);
                redis.SetRemove(schema.TriggerGroupsKey(), triggerGroupKey, CommandFlags.FireAndForget);
                redis.SetRemove(schema.TriggerGroupSetKey(triggerKey.Group), trigger, CommandFlags.FireAndForget);
                redis.KeyDelete(trigger.ToString(), CommandFlags.FireAndForget);
            }

            await Task.WhenAll(tasks);
            return await result;
        }

        public async Task<bool> RemoveTriggerAsync(TriggerKey triggerKey, bool removeNonDurableJob = true)
        {
            var triggerHashKey = schema.TriggerHashKey(triggerKey);

            if (!await redis.KeyExistsAsync(triggerHashKey))
            {
                return false;
            }

            var trigger = await RetrieveTriggerAsync(triggerKey);

            var triggerGroupSetKey = schema.TriggerGroupSetKey(triggerKey.Group);
            var jobTriggerSetKey = schema.JobTriggersKey(trigger.JobKey);

            redis.SetRemove(schema.TriggersKey(), triggerHashKey, CommandFlags.FireAndForget);
            redis.SetRemove(triggerGroupSetKey, triggerHashKey, CommandFlags.FireAndForget);
            redis.SetRemove(jobTriggerSetKey, triggerHashKey, CommandFlags.FireAndForget);
            var result = redis.KeyDeleteAsync(triggerHashKey);
            if (await redis.SetLengthAsync(triggerGroupSetKey) == 0)
            {
                redis.SetRemove(schema.TriggerGroupsKey(), triggerGroupSetKey, CommandFlags.FireAndForget);
            }

            var resetTriggerState = UnsetTriggerStateAsync(triggerHashKey);

            if (!string.IsNullOrEmpty(trigger.CalendarName))
            {
                redis.SetRemove(schema.CalendarTriggersKey(trigger.CalendarName), triggerHashKey, CommandFlags.FireAndForget);
            }

            if (removeNonDurableJob)
            {
                var jobTriggerSetKeyLengthResult = redis.SetLengthAsync(jobTriggerSetKey);

                var job = await RetrieveJobAsync(trigger.JobKey);

                if (await jobTriggerSetKeyLengthResult == 0 && job != null)
                {
                    if (!job.Durable)
                    {
                        Task.WaitAll(RemoveJobAsync(job.Key), schedulerSignaler.NotifySchedulerListenersJobDeleted(job.Key));
                    }
                }
            }

            await resetTriggerState;
            return await result;
        }

        public async Task<bool> ReplaceTriggerAsync(TriggerKey triggerKey, IOperableTrigger newTrigger)
        {
            var oldTrigger = await RetrieveTriggerAsync(triggerKey);

            if (oldTrigger == null)
            {
                return false;
            }

            if (!oldTrigger.JobKey.Equals(newTrigger.JobKey))
            {
                throw new JobPersistenceException("New Trigger is not linked to the same job as the old trigger");
            }

            Task.WaitAll(RemoveTriggerAsync(triggerKey, false), StoreTriggerAsync(newTrigger, false));

            return true;
        }

        public async Task ResumeAllTriggersAsync()
        {
            var triggerGroups = redis.SetMembersAsync(schema.TriggerGroupsKey());
            foreach (var group in await triggerGroups)
            {
                await ResumeTriggersAsync(GroupMatcher<TriggerKey>.GroupEquals(schema.TriggerGroup(group)));
            }
        }

        public async Task ResumeJobAsync(JobKey jobKey)
        {
            await Task.WhenAll((await GetTriggersForJobAsync(jobKey)).Select(trigger => ResumeTriggerAsync(trigger.Key)));
        }

        public async Task<IReadOnlyCollection<string>> ResumeJobsAsync(GroupMatcher<JobKey> matcher)
        {
            var resumedJobGroups = new List<string>();

            if (matcher.CompareWithOperator.Equals(StringOperator.Equality))
            {
                var jobGroupSetKey = schema.JobGroupKey(matcher.CompareToValue);

                var removedPausedResult = redis.SetRemove(schema.PausedJobGroupsKey(), jobGroupSetKey);
                var jobsResult = redis.SetMembers(jobGroupSetKey);

                if (removedPausedResult)
                {
                    resumedJobGroups.Add(schema.JobGroup(jobGroupSetKey));
                }

                var tasks = new List<Task>(jobsResult.Length);
                foreach (var job in jobsResult)
                {
                    tasks.Add(ResumeJobAsync(schema.JobKey(job)));
                }

                await Task.WhenAll(tasks);
            }
            else
            {
                foreach (var jobGroupSetKey in await redis.SetMembersAsync(schema.JobGroupsKey()))
                {
                    if (matcher.CompareWithOperator.Evaluate(schema.JobGroup(jobGroupSetKey), matcher.CompareToValue))
                    {
                        resumedJobGroups.AddRange(await ResumeJobsAsync(GroupMatcher<JobKey>.GroupEquals(schema.JobGroup(jobGroupSetKey))));
                    }
                }
            }

            return resumedJobGroups;
        }

        public async Task ResumeTriggerAsync(TriggerKey triggerKey)
        {
            var triggerHashKey = schema.TriggerHashKey(triggerKey);

            var isPausedTrigger = redis.SortedSetScoreAsync(schema.TriggerStateKey(TriggerStateEnum.Paused), triggerHashKey);
            var isPausedBlockedTrigger = redis.SortedSetScoreAsync(schema.TriggerStateKey(TriggerStateEnum.PausedBlocked), triggerHashKey);

            if (!(await isPausedTrigger).HasValue && !(await isPausedBlockedTrigger).HasValue)
            {
                return;
            }

            var trigger = await RetrieveTriggerAsync(triggerKey);

            var jobHashKey = schema.JobHashKey(trigger.JobKey);

            var nextFireTime = trigger.GetNextFireTimeUtc();

            if (nextFireTime.HasValue)
            {
                if (await redis.SetContainsAsync(schema.BlockedJobsSet(), jobHashKey))
                {
                    await SetTriggerStateAsync(TriggerStateEnum.Blocked, nextFireTime.Value.DateTime.ToUnixTimeMillieSeconds(), triggerHashKey);
                }
                else
                {
                    await SetTriggerStateAsync(TriggerStateEnum.Waiting, nextFireTime.Value.DateTime.ToUnixTimeMillieSeconds(), triggerHashKey);
                }
            }

            await ApplyMisfireAsync(trigger);
        }

        public async Task<IReadOnlyCollection<string>> ResumeTriggersAsync(GroupMatcher<TriggerKey> matcher)
        {
            var resumedTriggerGroups = new List<string>();

            if (matcher.CompareWithOperator.Equals(StringOperator.Equality))
            {
                var triggerGroupSetKey = schema.TriggerGroupSetKey(matcher.CompareToValue);

                redis.SetRemove(schema.PausedTriggerGroupsKey(), triggerGroupSetKey);

                var triggerHashKeysResult = redis.SetMembers(triggerGroupSetKey);

                var tasks = new List<Task>(triggerHashKeysResult.Length);
                foreach (var triggerHashKey in triggerHashKeysResult)
                {
                    var trigger = await RetrieveTriggerAsync(schema.TriggerKey(triggerHashKey));

                    tasks.Add(ResumeTriggerAsync(trigger.Key));

                    if (!resumedTriggerGroups.Contains(trigger.Key.Group))
                    {
                        resumedTriggerGroups.Add(trigger.Key.Group);
                    }
                }

                await Task.WhenAll(tasks);
            }
            else
            {
                foreach (var triggerGroupSetKy in redis.SetMembersAsync(schema.TriggerGroupsKey()).Result)
                {
                    if (matcher.CompareWithOperator.Evaluate(schema.TriggerGroup(triggerGroupSetKy), matcher.CompareToValue))
                    {
                        resumedTriggerGroups.AddRange(
                            await ResumeTriggersAsync(GroupMatcher<TriggerKey>.GroupEquals(schema.TriggerGroup(triggerGroupSetKy))));
                    }
                }
            }

            return resumedTriggerGroups;
        }

        public async Task<ICalendar> RetrieveCalendarAsync(string name)
        {
            var hashKey = schema.CalendarHashKey(name);
            ICalendar calendar = null;

            var calendarData = await redis.StringGetAsync(hashKey);

            if (calendarData.HasValue)
            {
                calendar = JsonDeSerialize(calendarData) as ICalendar;
            }

            return calendar;
        }

        public async Task<IJobDetail> RetrieveJobAsync(JobKey jobKey)
        {
            var key = schema.JobHashKey(jobKey);

            var detail = await redis.HashGetAllAsync(key);

            if (detail == null || detail.Length == 0)
            {
                return null;
            }

            var data = await redis.HashGetAllAsync(schema.JobDataMapHashKey(jobKey));

            var properties = detail.ToStringDictionary();

            var builder = JobBuilder.Create(Type.GetType(properties[JobStoreKeyEnum.JobClass]))
                                    .WithIdentity(jobKey)
                                    .WithDescription(properties[JobStoreKeyEnum.Description])
                                    .RequestRecovery(Convert.ToBoolean(Convert.ToInt16(properties[JobStoreKeyEnum.RequestRecovery])))
                                    .StoreDurably(Convert.ToBoolean(Convert.ToInt16(properties[JobStoreKeyEnum.IsDurable])));

            if (data != null && data.Length > 0)
            {
                builder.SetJobData(new JobDataMap(data.ToStringDictionary()));
            }

            return builder.Build();
        }

        public async Task<IOperableTrigger> RetrieveTriggerAsync(TriggerKey triggerKey)
        {
            var triggerHashKey = schema.TriggerHashKey(triggerKey);

            var properties = redis.HashGetAllAsync(triggerHashKey);

            if (properties != null && (await properties).Length > 0)
            {
                return RetrieveTrigger(triggerKey, ConvertToDictionaryString(await properties));
            }

            return null;
        }

        public async Task StoreCalendarAsync(string name, ICalendar calendar, bool replaceExisting, bool updateTriggers)
        {
            var hashKey = schema.CalendarHashKey(name);

            if (!replaceExisting && await redis.KeyExistsAsync(hashKey))
            {
                throw new ObjectAlreadyExistsException(string.Format("Calendar with key {0} already exists", hashKey));
            }

            redis.StringSet(hashKey, JsonSerialize(calendar), flags: CommandFlags.FireAndForget);
            redis.SetAdd(schema.CalendarsKey(), hashKey, CommandFlags.FireAndForget);

            if (updateTriggers)
            {
                var calendarTrigger = schema.CalendarTriggersKey(name);

                var triggerHashKeys = redis.SetMembersAsync(calendarTrigger);

                foreach (var triggerKey in await triggerHashKeys)
                {
                    var trigger = RetrieveTriggerAsync(schema.TriggerKey(triggerKey));

                    (await trigger).UpdateWithNewCalendar(calendar, TimeSpan.FromSeconds(misfireThreshold));

                    await StoreTriggerAsync(await trigger, true);
                }
            }
        }

        public async Task StoreJobAsync(IJobDetail jobDetail, bool replaceExisting)
        {
            var jobHashKey = schema.JobHashKey(jobDetail.Key);
            var jobDataMapHashKey = schema.JobDataMapHashKey(jobDetail.Key);
            var jobGroupSetKey = schema.JobGroupKey(jobDetail.Key.Group);

            if (await redis.KeyExistsAsync(jobHashKey) && !replaceExisting)
            {
                throw new ObjectAlreadyExistsException(jobDetail);
            }

            redis.HashSet(jobHashKey, ConvertToHashEntries(jobDetail), CommandFlags.FireAndForget);
            redis.HashSet(jobDataMapHashKey, ConvertToHashEntries(jobDetail.JobDataMap), CommandFlags.FireAndForget);
            redis.SetAdd(schema.JobsKey(), jobHashKey, CommandFlags.FireAndForget);
            redis.SetAdd(schema.JobGroupsKey(), jobGroupSetKey, CommandFlags.FireAndForget);
            redis.SetAdd(jobGroupSetKey, jobHashKey, CommandFlags.FireAndForget);
        }

        public async Task StoreTriggerAsync(ITrigger trigger, bool replaceExisting)
        {
            if (!(trigger is ISimpleTrigger || trigger is ICronTrigger))
            {
                throw new NotImplementedException("Unknown trigger, only SimpleTrigger and CronTrigger are supported");
            }

            var hashKey = schema.TriggerHashKey(trigger.Key);
            var triggerGroup = schema.TriggerGroupSetKey(trigger.Key.Group);
            var jobTriggers = schema.JobTriggersKey(trigger.JobKey);

            var triggerExists = await redis.KeyExistsAsync(hashKey);

            if (triggerExists && !replaceExisting)
            {
                throw new ObjectAlreadyExistsException(trigger);
            }

            redis.HashSet(hashKey, ConvertToHashEntries(trigger), CommandFlags.FireAndForget);
            redis.SetAdd(schema.TriggersKey(), hashKey, CommandFlags.FireAndForget);
            redis.SetAdd(schema.TriggerGroupsKey(), triggerGroup, CommandFlags.FireAndForget);
            redis.SetAdd(triggerGroup, hashKey, CommandFlags.FireAndForget);
            redis.SetAdd(jobTriggers, hashKey, CommandFlags.FireAndForget);

            if (!string.IsNullOrEmpty(trigger.CalendarName))
            {
                var calendarTriggersSetKey = schema.CalendarTriggersKey(trigger.CalendarName);
                redis.SetAdd(calendarTriggersSetKey, hashKey, CommandFlags.FireAndForget);
            }

            await UpdateTriggerStateAsync(trigger);
        }

        public double ToUnixTimeMilliseconds(DateTimeOffset dateTimeOffset)
        {
            // Truncate sub-millisecond precision before offsetting by the Unix Epoch to avoid
            // the last digit being off by one for dates that result in negative Unix times
            return (dateTimeOffset - new DateTimeOffset(unixEpoch)).TotalMilliseconds;
        }

        public async Task TriggeredJobCompleteAsync(IOperableTrigger trigger, IJobDetail jobDetail, SchedulerInstruction triggerInstCode)
        {
            var jobHashKey = schema.JobHashKey(jobDetail.Key);

            var jobDataMapHashKey = schema.JobDataMapHashKey(jobDetail.Key);

            var triggerHashKey = schema.TriggerHashKey(trigger.Key);

            if (await redis.KeyExistsAsync(jobHashKey))
            {
                //Logger.InfoFormat("{0} - Job has completed", jobHashKey);
                if (jobDetail.PersistJobDataAfterExecution)
                {
                    var jobDataMap = jobDetail.JobDataMap;

                    redis.KeyDelete(jobDataMapHashKey, CommandFlags.FireAndForget);
                    if (jobDataMap != null && !jobDataMap.IsEmpty)
                    {
                        redis.HashSet(jobDataMapHashKey, ConvertToHashEntries(jobDataMap), CommandFlags.FireAndForget);
                    }
                }

                if (jobDetail.ConcurrentExecutionDisallowed)
                {
                    redis.SetRemove(schema.BlockedJobsSet(), jobHashKey, CommandFlags.FireAndForget);

                    redis.KeyDelete(schema.JobBlockedKey(jobDetail.Key), CommandFlags.FireAndForget);

                    var jobTriggersSetKey = schema.JobTriggersKey(jobDetail.Key);

                    foreach (var nonConcurrentTriggerHashKey in await redis.SetMembersAsync(jobTriggersSetKey))
                    {
                        var score = await redis.SortedSetScoreAsync(schema.TriggerStateKey(TriggerStateEnum.Blocked), nonConcurrentTriggerHashKey);
                        if (score.HasValue)
                        {
                            await SetTriggerStateAsync(TriggerStateEnum.Paused, score.Value, nonConcurrentTriggerHashKey);
                        }
                        else
                        {
                            score = redis.SortedSetScoreAsync(schema.TriggerStateKey(TriggerStateEnum.PausedBlocked), nonConcurrentTriggerHashKey)
                                         .Result;

                            if (score.HasValue)
                            {
                                await SetTriggerStateAsync(TriggerStateEnum.Paused, score.Value, nonConcurrentTriggerHashKey);
                            }
                        }
                    }

                    schedulerSignaler.SignalSchedulingChange(null);
                }
            }
            else
            {
                redis.SetRemove(schema.BlockedJobsSet(), jobHashKey, CommandFlags.FireAndForget);
            }

            if (await redis.KeyExistsAsync(triggerHashKey))
            {
                if (triggerInstCode == SchedulerInstruction.DeleteTrigger)
                {
                    if (trigger.GetNextFireTimeUtc().HasValue == false)
                    {
                        if (string.IsNullOrEmpty(await redis.HashGetAsync(triggerHashKey, TriggerStoreKeyEnum.NextFireTime)))
                        {
                            await RemoveTriggerAsync(trigger.Key);
                        }
                    }
                    else
                    {
                        await RemoveTriggerAsync(trigger.Key);
                        schedulerSignaler.SignalSchedulingChange(null);
                    }
                }
                else if (triggerInstCode == SchedulerInstruction.SetTriggerComplete)
                {
                    await SetTriggerStateAsync(TriggerStateEnum.Completed, DateTimeOffset.UtcNow.DateTime.ToUnixTimeMillieSeconds(), triggerHashKey);
                    schedulerSignaler.SignalSchedulingChange(null);
                }
                else if (triggerInstCode == SchedulerInstruction.SetTriggerError)
                {
                    var score = trigger.GetNextFireTimeUtc().HasValue ? trigger.GetNextFireTimeUtc().GetValueOrDefault().DateTime.ToUnixTimeMillieSeconds() : 0;
                    await SetTriggerStateAsync(TriggerStateEnum.Error, score, triggerHashKey);
                    schedulerSignaler.SignalSchedulingChange(null);
                }
                else if (triggerInstCode == SchedulerInstruction.SetAllJobTriggersError)
                {
                    var jobTriggersSetKey = schema.JobTriggersKey(jobDetail.Key);

                    foreach (var errorTriggerHashKey in await redis.SetMembersAsync(jobTriggersSetKey))
                    {
                        var nextFireTime = redis.HashGetAsync(errorTriggerHashKey.ToString(), TriggerStoreKeyEnum.NextFireTime);
                        var score = string.IsNullOrEmpty(await nextFireTime) ? 0 : double.Parse(await nextFireTime);
                        await SetTriggerStateAsync(TriggerStateEnum.Error, score, errorTriggerHashKey);
                    }

                    schedulerSignaler.SignalSchedulingChange(null);
                }
                else if (triggerInstCode == SchedulerInstruction.SetAllJobTriggersComplete)
                {
                    var jobTriggerSetKey = schema.JobTriggersKey(jobDetail.Key);
                    var triggers = redis.SetMembersAsync(jobTriggerSetKey);
                    await Task.WhenAll(
                        (await triggers).Select(
                            s => SetTriggerStateAsync(TriggerStateEnum.Completed, DateTimeOffset.UtcNow.DateTime.ToUnixTimeMillieSeconds(), s)));

                    schedulerSignaler.SignalSchedulingChange(null);
                }
            }
        }

        public async Task<IReadOnlyCollection<string>> TriggerGroupNamesAsync()
        {
            var groupsSet = redis.SetMembersAsync(schema.TriggerGroupsKey());

            return (await groupsSet).Select(s => schema.TriggerGroup(s)).ToList();
        }

        public async Task<IReadOnlyCollection<TriggerKey>> TriggerKeysAsync(GroupMatcher<TriggerKey> matcher)
        {
            var triggerKeys = new List<TriggerKey>();

            var triggerGroupSets = redis.SetMembersAsync(schema.TriggerGroupsKey());
            var triggerGroupsResult = (await triggerGroupSets)
                                     .Where(x => matcher.CompareWithOperator.Evaluate(schema.TriggerGroup(x), matcher.CompareToValue))
                                     .Select(s => redis.SetMembersAsync(s.ToString()));

            foreach (var triggerHashKeys in triggerGroupsResult)
            {
                if (await triggerHashKeys == null)
                {
                    continue;
                }

                triggerKeys.AddRange(from triggerHashKey in await triggerHashKeys select schema.TriggerKey(triggerHashKey));
            }

            return triggerKeys;
        }

        public async Task<IReadOnlyCollection<TriggerFiredResult>> TriggersFiredAsync(IEnumerable<IOperableTrigger> triggers)
        {
            var result = new List<TriggerFiredResult>();

            foreach (var trigger in triggers)
            {
                var triggerHashKey = schema.TriggerHashKey(trigger.Key);

                var triggerExistResult = redis.KeyExistsAsync(triggerHashKey);
                var triggerAcquiredResult = redis.SortedSetScoreAsync(schema.TriggerStateKey(TriggerStateEnum.Acquired), triggerHashKey);

                if (!await triggerExistResult)
                {
                    //Logger.WarnFormat("Trigger {0} does not exist", triggerHashKey);
                    continue;
                }

                if (!(await triggerAcquiredResult).HasValue)
                {
                    //Logger.WarnFormat("Trigger {0} was not acquired", triggerHashKey);
                    continue;
                }

                ICalendar calendar = null;

                var calendarName = trigger.CalendarName;
                if (!string.IsNullOrEmpty(calendarName))
                {
                    calendar = await RetrieveCalendarAsync(calendarName);

                    if (calendar == null)
                    {
                        continue;
                    }
                }

                var previousFireTime = trigger.GetPreviousFireTimeUtc();

                trigger.Triggered(calendar);

                var job = await RetrieveJobAsync(trigger.JobKey);

                var triggerFireBundle = new TriggerFiredBundle(
                    job,
                    trigger,
                    calendar,
                    false,
                    DateTimeOffset.UtcNow,
                    previousFireTime,
                    previousFireTime,
                    trigger.GetNextFireTimeUtc());

                if (job.ConcurrentExecutionDisallowed)
                {
                    var jobHasKey = schema.JobHashKey(trigger.JobKey);
                    var jobTriggerSetKey = schema.JobTriggersKey(job.Key);
                    var jobSet = await redis.SetMembersAsync(jobTriggerSetKey);
                    var tasks = new List<Task>(jobSet.Length);
                    foreach (var nonConcurrentTriggerHashKey in jobSet)
                    {
                        var score = redis.SortedSetScore(schema.TriggerStateKey(TriggerStateEnum.Waiting), nonConcurrentTriggerHashKey);

                        if (score.HasValue)
                        {
                            tasks.Add(SetTriggerStateAsync(TriggerStateEnum.Blocked, score.Value, nonConcurrentTriggerHashKey));
                        }
                        else
                        {
                            score = await redis.SortedSetScoreAsync(schema.TriggerStateKey(TriggerStateEnum.Paused), nonConcurrentTriggerHashKey);
                            if (score.HasValue)
                            {
                                tasks.Add(SetTriggerStateAsync(TriggerStateEnum.PausedBlocked, score.Value, nonConcurrentTriggerHashKey));
                            }
                        }
                    }

                    Task.WaitAll(
                        redis.SetAddAsync(schema.JobBlockedKey(job.Key), schedulerInstanceId),
                        redis.SetAddAsync(schema.BlockedJobsSet(), jobHasKey));
                }

                //release the fired triggers
                var nextFireTimeUtc = trigger.GetNextFireTimeUtc();
                if (nextFireTimeUtc != null)
                {
                    var nextFireTime = nextFireTimeUtc.Value;
                    redis.HashSet(triggerHashKey, TriggerStoreKeyEnum.NextFireTime, nextFireTime.DateTime.ToUnixTimeMillieSeconds(), flags: CommandFlags.FireAndForget);
                    await SetTriggerStateAsync(TriggerStateEnum.Waiting, nextFireTime.DateTime.ToUnixTimeMillieSeconds(), triggerHashKey);
                }
                else
                {
                    redis.HashSet(triggerHashKey, TriggerStoreKeyEnum.NextFireTime, string.Empty, flags: CommandFlags.FireAndForget);
                    await UnsetTriggerStateAsync(triggerHashKey);
                }

                result.Add(new TriggerFiredResult(triggerFireBundle));
            }

            return result;
        }

        public Task<bool> UnlockAsync()
        {
            return redis.LockReleaseAsync(schema.LockKey, lockValue);
        }

        public async Task<bool> UnsetTriggerStateAsync(string triggerHashKey)
        {
            foreach (TriggerStateEnum state in Enum.GetValues(typeof(TriggerStateEnum)))
            {
                var hasKey = redis.SortedSetRemoveAsync(schema.TriggerStateKey(state), triggerHashKey);
                if (await hasKey)
                {
                    return await redis.KeyDeleteAsync(schema.TriggerLockKey(schema.TriggerKey(triggerHashKey)));
                }
            }

            return false;
        }

        protected async Task<bool> ApplyMisfireAsync(IOperableTrigger trigger)
        {
            var misfireTime = DateTimeOffset.UtcNow.DateTime.ToUnixTimeMillieSeconds();
            var score = misfireTime;

            if (misfireThreshold > 0)
            {
                misfireTime = misfireTime - misfireThreshold;
            }

            //if the trigger has no next fire time or exceeds the misfirethreshold or enable ignore misfirepolicy
            // then dont apply misfire.
            var nextFireTime = trigger.GetNextFireTimeUtc();

            if ((nextFireTime.HasValue && nextFireTime.Value.DateTime.ToUnixTimeMillieSeconds() > misfireTime)
                || trigger.MisfireInstruction == -1)
            {
                return false;
            }

            ICalendar calendar = null;

            if (!string.IsNullOrEmpty(trigger.CalendarName))
            {
                calendar = await RetrieveCalendarAsync(trigger.CalendarName);
            }

            await schedulerSignaler.NotifyTriggerListenersMisfired((IOperableTrigger)trigger.Clone());

            trigger.UpdateAfterMisfire(calendar);

            await StoreTriggerAsync(trigger, true);

            if (!nextFireTime.HasValue)
            {
                await SetTriggerStateAsync(TriggerStateEnum.Completed, score, schema.TriggerHashKey(trigger.Key));
                await schedulerSignaler.NotifySchedulerListenersFinalized(trigger);
            }

            if (nextFireTime.Equals(trigger.GetNextFireTimeUtc()))
            {
                return false;
            }

            return true;
        }

        protected IDictionary<string, string> ConvertToDictionaryString(HashEntry[] entries)
        {
            return entries.ToStringDictionary();
        }

        protected HashEntry[] ConvertToHashEntries(JobDataMap jobDataMap)
        {
            if (jobDataMap == null)
            {
                return new HashEntry[0];
            }

            var entries = new HashEntry[jobDataMap.Count];
            var i = 0;
            foreach (var entry in jobDataMap)
            {
                entries[i++] = new HashEntry(entry.Key, entry.Value.ToString());
            }

            return entries;
        }

        protected HashEntry[] ConvertToHashEntries(IJobDetail jobDetail)
        {
            return new[]
                       {
                           new HashEntry(JobStoreKeyEnum.JobClass, jobDetail.JobType.AssemblyQualifiedName),
                           new HashEntry(JobStoreKeyEnum.Description, jobDetail.Description ?? string.Empty),
                           new HashEntry(JobStoreKeyEnum.IsDurable, jobDetail.Durable),
                           new HashEntry(JobStoreKeyEnum.RequestRecovery, jobDetail.RequestsRecovery),
                           new HashEntry(JobStoreKeyEnum.BlockedBy, string.Empty),
                           new HashEntry(JobStoreKeyEnum.BlockTime, string.Empty)
                       };
        }

        protected HashEntry[] ConvertToHashEntries(ITrigger trigger)
        {
            var operable = (IOperableTrigger)trigger;
            if (operable == null)
            {
                throw new InvalidCastException("trigger needs to be IOperable");
            }

            var entries = new List<HashEntry>
                              {
                                  new HashEntry(
                                      TriggerStoreKeyEnum.JobHash,
                                      operable.JobKey == null ? string.Empty : schema.JobHashKey(operable.JobKey)),
                                  new HashEntry(TriggerStoreKeyEnum.Description, operable.Description ?? string.Empty),
                                  new HashEntry(
                                      TriggerStoreKeyEnum.NextFireTime,
                                      operable.GetNextFireTimeUtc().HasValue
                                          ? operable.GetNextFireTimeUtc().GetValueOrDefault().DateTime.ToUnixTimeMillieSeconds().ToString(CultureInfo.InvariantCulture)
                                          : string.Empty),
                                  new HashEntry(
                                      TriggerStoreKeyEnum.PrevFireTime,
                                      operable.GetPreviousFireTimeUtc().HasValue
                                          ? operable.GetPreviousFireTimeUtc().GetValueOrDefault().DateTime.ToUnixTimeMillieSeconds().ToString(CultureInfo.InvariantCulture)
                                          : string.Empty),
                                  new HashEntry(TriggerStoreKeyEnum.Priority, operable.Priority),
                                  new HashEntry(
                                      TriggerStoreKeyEnum.StartTime,
                                      operable.StartTimeUtc.DateTime.ToUnixTimeMillieSeconds()),
                                  new HashEntry(
                                      TriggerStoreKeyEnum.EndTime,
                                      operable.EndTimeUtc.HasValue
                                          ? operable.EndTimeUtc.Value.DateTime.ToUnixTimeMillieSeconds().ToString(CultureInfo.InvariantCulture)
                                          : string.Empty),
                                  new HashEntry(
                                      TriggerStoreKeyEnum.FinalFireTime,
                                      operable.FinalFireTimeUtc.HasValue
                                          ? operable.FinalFireTimeUtc.Value.DateTime.ToUnixTimeMillieSeconds().ToString(CultureInfo.InvariantCulture)
                                          : string.Empty),
                                  new HashEntry(TriggerStoreKeyEnum.FireInstanceId, operable.FireInstanceId ?? string.Empty),
                                  new HashEntry(TriggerStoreKeyEnum.MisfireInstruction, operable.MisfireInstruction),
                                  new HashEntry(TriggerStoreKeyEnum.CalendarName, operable.CalendarName ?? string.Empty)
                              };

            switch (operable)
            {
                case ISimpleTrigger _:
                    entries.Add(new HashEntry(TriggerStoreKeyEnum.TriggerType, TriggerStoreKeyEnum.TriggerTypeSimple));
                    entries.Add(new HashEntry(TriggerStoreKeyEnum.RepeatCount, ((ISimpleTrigger)operable).RepeatCount));
                    entries.Add(new HashEntry(TriggerStoreKeyEnum.RepeatInterval, ((ISimpleTrigger)operable).RepeatInterval.ToString()));
                    entries.Add(new HashEntry(TriggerStoreKeyEnum.TimesTriggered, ((ISimpleTrigger)operable).TimesTriggered));
                    break;
                case ICronTrigger _:
                    entries.Add(new HashEntry(TriggerStoreKeyEnum.TriggerType, TriggerStoreKeyEnum.TriggerTypeCron));
                    entries.Add(new HashEntry(TriggerStoreKeyEnum.CronExpression, ((ICronTrigger)operable).CronExpressionString));
                    entries.Add(new HashEntry(TriggerStoreKeyEnum.TimeZoneId, ((ICronTrigger)operable).TimeZone.Id));
                    break;
            }

            return entries.ToArray();
        }

        protected async Task<double> GetLastTriggersReleaseTimeAsync()
        {
            var lastReleaseTime = redis.StringGetAsync(schema.LastTriggerReleaseTime());

            return string.IsNullOrEmpty(await lastReleaseTime) ? 0 : double.Parse(await lastReleaseTime);
        }

        protected async Task<bool> LockTriggerAsync(TriggerKey triggerKey)
        {
            return await redis.StringSetAsync(schema.TriggerLockKey(triggerKey), schedulerInstanceId, TimeSpan.FromSeconds(triggerLockTimeout));
        }

        protected int RandomInt(int min, int max)
        {
            return new Random().Next(max - min + 1) + min;
        }

        protected async Task ReleaseOrphanedTriggersAsync(TriggerStateEnum currentState, TriggerStateEnum newState)
        {
            var triggers = redis.SortedSetRangeByScoreWithScoresAsync(schema.TriggerStateKey(currentState), 0, -1);

            foreach (var sortedSetEntry in await triggers)
            {
                var lockedId = redis.StringGetAsync(schema.TriggerLockKey(schema.TriggerKey(sortedSetEntry.Element.ToString())));
                if (string.IsNullOrEmpty(await lockedId))
                {
                    await SetTriggerStateAsync(newState, sortedSetEntry.Score, sortedSetEntry.Element);
                }
            }
        }

        protected async Task ReleaseTriggersAsync()
        {
            var misfireTime = DateTimeOffset.UtcNow.DateTime.ToUnixTimeMillieSeconds();
            if (misfireTime - await GetLastTriggersReleaseTimeAsync() > triggerLockTimeout)
            {
                Task.WaitAll(
                    ReleaseOrphanedTriggersAsync(TriggerStateEnum.Acquired, TriggerStateEnum.Waiting),
                    ReleaseOrphanedTriggersAsync(TriggerStateEnum.Blocked, TriggerStateEnum.Waiting),
                    ReleaseOrphanedTriggersAsync(TriggerStateEnum.PausedBlocked, TriggerStateEnum.Paused));
                await SetLastTriggerReleaseTimeAsync(DateTimeOffset.UtcNow.DateTime.ToUnixTimeMillieSeconds());
            }
        }

        protected Task SetLastTriggerReleaseTimeAsync(double time)
        {
            return redis.StringSetAsync(schema.LastTriggerReleaseTime(), time);
        }

        protected async Task<bool> SetTriggerStateAsync(TriggerStateEnum state, double score, string triggerHashKey)
        {
            await UnsetTriggerStateAsync(triggerHashKey);
            return await redis.SortedSetAddAsync(schema.TriggerStateKey(state), triggerHashKey, score);
        }

        private DateTime DateTimeFromUnixTimestampMillis(double millis)
        {
            return unixEpoch.AddMilliseconds(millis);
        }

        private object JsonDeSerialize(string jsonString)
        {
            object result;

            using (var ms = new MemoryStream(Encoding.UTF8.GetBytes(jsonString)))
            using (var sr = new StreamReader(ms))
            using (var reader = new JsonTextReader(sr))
            {
                result = serializer.Deserialize(reader);
            }

            return result;
        }

        private string JsonSerialize<T>(T obj)
        {
            string result;

            using (var ms = new MemoryStream())
            using (var sw = new StreamWriter(ms))
            using (var jsonTextWriter = new JsonTextWriter(sw))
            using (var reader = new StreamReader(ms, Encoding.UTF8))
            {
                serializer.Serialize(jsonTextWriter, obj);
                jsonTextWriter.Flush();
                ms.Seek(0, SeekOrigin.Begin);
                result = reader.ReadToEnd();
            }

            return result;
        }

        private async Task<bool> LockAsync()
        {
            var id = Guid.NewGuid().ToString();

            var reacquired = await redis.LockTakeAsync(schema.LockKey, id, TimeSpan.FromMilliseconds(redisLockTimeout));
            if (reacquired)
            {
                lockValue = id;
            }

            return reacquired;
        }

        private void PopulateTrigger(TriggerKey triggerKey, IDictionary<string, string> properties, IOperableTrigger trigger)
        {
            trigger.Key = triggerKey;
            trigger.JobKey = schema.JobKey(properties[TriggerStoreKeyEnum.JobHash]);
            trigger.Description = properties[TriggerStoreKeyEnum.Description];
            trigger.FireInstanceId = properties[TriggerStoreKeyEnum.FireInstanceId];
            trigger.CalendarName = properties[TriggerStoreKeyEnum.CalendarName];
            trigger.Priority = int.Parse(properties[TriggerStoreKeyEnum.Priority]);
            trigger.MisfireInstruction = int.Parse(properties[TriggerStoreKeyEnum.MisfireInstruction]);
            trigger.StartTimeUtc = DateTimeFromUnixTimestampMillis(double.Parse(properties[TriggerStoreKeyEnum.StartTime]));

            trigger.EndTimeUtc = string.IsNullOrEmpty(properties[TriggerStoreKeyEnum.EndTime])
                                     ? default(DateTimeOffset?)
                                     : DateTimeFromUnixTimestampMillis(double.Parse(properties[TriggerStoreKeyEnum.EndTime]));

            if (trigger is AbstractTrigger)
            {
                trigger.SetNextFireTimeUtc(
                    string.IsNullOrEmpty(properties[TriggerStoreKeyEnum.NextFireTime])
                        ? default(DateTimeOffset?)
                        : DateTimeFromUnixTimestampMillis(double.Parse(properties[TriggerStoreKeyEnum.NextFireTime])));

                trigger.SetPreviousFireTimeUtc(
                    string.IsNullOrEmpty(properties[TriggerStoreKeyEnum.PrevFireTime])
                        ? default(DateTimeOffset?)
                        : DateTimeFromUnixTimestampMillis(double.Parse(properties[TriggerStoreKeyEnum.PrevFireTime])));
            }
        }

        private IOperableTrigger RetrieveTrigger(TriggerKey triggerKey, IDictionary<string, string> properties)
        {
            var type = properties[TriggerStoreKeyEnum.TriggerType];

            if (string.IsNullOrEmpty(type))
            {
                return null;
            }

            if (type.Equals(TriggerStoreKeyEnum.TriggerTypeSimple, StringComparison.OrdinalIgnoreCase))
            {
                var simpleTrigger = new SimpleTriggerImpl();

                if (!string.IsNullOrEmpty(properties[TriggerStoreKeyEnum.RepeatCount]))
                {
                    simpleTrigger.RepeatCount = Convert.ToInt32(properties[TriggerStoreKeyEnum.RepeatCount]);
                }

                if (!string.IsNullOrEmpty(properties[TriggerStoreKeyEnum.RepeatInterval]))
                {
                    simpleTrigger.RepeatInterval = TimeSpan.Parse(properties[TriggerStoreKeyEnum.RepeatInterval]);
                }

                if (!string.IsNullOrEmpty(properties[TriggerStoreKeyEnum.TimesTriggered]))
                {
                    simpleTrigger.TimesTriggered = Convert.ToInt32(properties[TriggerStoreKeyEnum.TimesTriggered]);
                }

                PopulateTrigger(triggerKey, properties, simpleTrigger);

                return simpleTrigger;
            }

            var cronTrigger = new CronTriggerImpl();

            if (!string.IsNullOrEmpty(properties[TriggerStoreKeyEnum.TimeZoneId]))
            {
                cronTrigger.TimeZone = TimeZoneInfo.FindSystemTimeZoneById(properties[TriggerStoreKeyEnum.TimeZoneId]);
            }

            if (!string.IsNullOrEmpty(properties[TriggerStoreKeyEnum.CronExpression]))
            {
                cronTrigger.CronExpressionString = properties[TriggerStoreKeyEnum.CronExpression];
            }

            PopulateTrigger(triggerKey, properties, cronTrigger);

            return cronTrigger;
        }

        private async Task UpdateTriggerStateAsync(ITrigger trigger)
        {
            var triggerPausedResult = redis.SetContainsAsync(schema.PausedTriggerGroupsKey(), schema.TriggerGroupSetKey(trigger.Key.Group));
            var jobPausedResult = redis.SetContainsAsync(schema.PausedJobGroupsKey(), schema.JobGroupKey(trigger.JobKey.Group));
            var nextFireTime = trigger.GetNextFireTimeUtc().HasValue
                                   ? trigger.GetNextFireTimeUtc().GetValueOrDefault().DateTime.ToUnixTimeMillieSeconds()
                                   : -1;
            if (await triggerPausedResult || await jobPausedResult)
            {
                var jobHashKey = schema.JobHashKey(trigger.JobKey);

                if (await redis.SetContainsAsync(schema.BlockedJobsSet(), jobHashKey))
                {
                    await SetTriggerStateAsync(TriggerStateEnum.PausedBlocked, nextFireTime, schema.TriggerHashKey(trigger.Key));
                }
                else
                {
                    await SetTriggerStateAsync(TriggerStateEnum.Paused, nextFireTime, schema.TriggerHashKey(trigger.Key));
                }
            }
            else if (trigger.GetNextFireTimeUtc().HasValue)
            {
                await SetTriggerStateAsync(TriggerStateEnum.Waiting, nextFireTime, schema.TriggerHashKey(trigger.Key));
            }
        }
    }
}