namespace Quartz.RedisJobStore.Async
{
    #region

    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    using Common.Logging;

    using Quartz.Impl.Matchers;
    using Quartz.Spi;

    using StackExchange.Redis;

    #endregion

    public class RedisJobStore : IJobStore
    {
        private readonly ILog logger;

        private RedisStorage storage;

        public RedisJobStore()
        {
            logger = logger = LogManager.GetLogger<RedisJobStore>();
        }

        public bool Clustered => true;

        public long EstimatedTimeToReleaseAndAcquireTrigger => 200;

        public string InstanceId { get; set; }

        public string InstanceName { get; set; }

        public string KeyDelimiter { get; set; }

        public int? MisfireThreshold { get; set; }

        public Task<ConnectionMultiplexer> Multiplexer { get; set; }

        public string RedisConfiguration { get; set; }
        
        public bool SupportsPersistence => true;

        public int ThreadPoolSize { get; set; }
        
        public Task<IReadOnlyCollection<IOperableTrigger>> AcquireNextTriggers(
            DateTimeOffset noLaterThan,
            int maxCount,
            TimeSpan timeWindow,
            CancellationToken cancellationToken = new CancellationToken())
        {
            return DoWithLock(storage.AcquireNextTriggersAsync(noLaterThan, maxCount, timeWindow), "Error on acquiring next triggers");
        }

        public Task<bool> CalendarExists(string calName, CancellationToken cancellationToken = new CancellationToken())
        {
            return DoWithLock(storage.CheckExistsAsync(calName), $"could not check if the calendar {calName} exists");
        }

        public Task<bool> CheckExists(JobKey jobKey, CancellationToken cancellationToken = new CancellationToken())
        {
            return DoWithLock(storage.CheckExistsAsync(jobKey), $"could not check if the job {jobKey} exists");
        }

        public Task<bool> CheckExists(TriggerKey triggerKey, CancellationToken cancellationToken = new CancellationToken())
        {
            return DoWithLock(storage.CheckExistsAsync(triggerKey), $"could not check if the trigger {triggerKey} exists");
        }

        public Task ClearAllSchedulingData(CancellationToken cancellationToken = new CancellationToken())
        {
            return DoWithLock(storage.ClearAllSchedulingDataAsync(), "Could not clear all the scheduling data");
        }

        public Task<IReadOnlyCollection<string>> GetCalendarNames(CancellationToken cancellationToken = new CancellationToken())
        {
            return DoWithLock(storage.CalendarNamesAsync(), "Error on getting calendar names");
        }

        public Task<IReadOnlyCollection<string>> GetJobGroupNames(CancellationToken cancellationToken = new CancellationToken())
        {
            return DoWithLock(storage.JobGroupNamesAsync(), "Error on getting calendar names");
        }

        public Task<IReadOnlyCollection<JobKey>> GetJobKeys(
            GroupMatcher<JobKey> matcher,
            CancellationToken cancellationToken = new CancellationToken())
        {
            return DoWithLock(storage.GetJobKeysAsync(matcher), "Error on getting job keys");
        }

        public Task<int> GetNumberOfCalendars(CancellationToken cancellationToken = new CancellationToken())
        {
            return DoWithLock(storage.NumberOfCalendarsAsync(), "Error on getting number of calendars");
        }

        public Task<int> GetNumberOfJobs(CancellationToken cancellationToken = new CancellationToken())
        {
            return DoWithLock(storage.NumberOfJobsAsync(), "Error on getting number of jobs");
        }

        public Task<int> GetNumberOfTriggers(CancellationToken cancellationToken = new CancellationToken())
        {
            return DoWithLock(storage.NumberOfTriggersAsync(), "Error on getting number of triggers");
        }

        public Task<IReadOnlyCollection<string>> GetPausedTriggerGroups(CancellationToken cancellationToken = new CancellationToken())
        {
            return DoWithLock(storage.GetPausedTriggerGroupsAsync(), "Error on getting paused trigger groups");
        }

        public Task<IReadOnlyCollection<string>> GetTriggerGroupNames(CancellationToken cancellationToken = new CancellationToken())
        {
            return DoWithLock(storage.TriggerGroupNamesAsync(), "Error on getting trigger group names");
        }

        public Task<IReadOnlyCollection<TriggerKey>> GetTriggerKeys(
            GroupMatcher<TriggerKey> matcher,
            CancellationToken cancellationToken = new CancellationToken())
        {
            return DoWithLock(storage.TriggerKeysAsync(matcher), "Error on getting trigger keys");
        }

        public Task<IReadOnlyCollection<IOperableTrigger>> GetTriggersForJob(
            JobKey jobKey,
            CancellationToken cancellationToken = new CancellationToken())
        {
            return DoWithLock(storage.GetTriggersForJobAsync(jobKey), $"Error on getting triggers for job - {jobKey}");
        }

        public Task<TriggerState> GetTriggerState(TriggerKey triggerKey, CancellationToken cancellationToken = new CancellationToken())
        {
            return DoWithLock(storage.GetTriggerStateAsync(triggerKey), $"Error on getting trigger state for trigger - {triggerKey}");
        }

        public async Task Initialize(
            ITypeLoadHelper loadHelper,
            ISchedulerSignaler signaler,
            CancellationToken cancellationToken = new CancellationToken())
        {
            var sw = Stopwatch.StartNew();
            var schema = new RedisKeySchema(string.IsNullOrEmpty(KeyDelimiter) ? ":" : KeyDelimiter);
            Multiplexer = ConnectionMultiplexer.ConnectAsync(RedisConfiguration);
            storage = new RedisStorage(
                schema,
                (await Multiplexer).GetDatabase(),
                signaler,
                InstanceId,
                MisfireThreshold ?? 60000);

            logger.Debug($"Initialize Done - Elapsed{sw.Elapsed:g}");
        }

        public Task<bool> IsJobGroupPaused(string groupName, CancellationToken cancellationToken = new CancellationToken())
        {
            return DoWithLock(storage.IsJobGroupPausedAsync(groupName), $"Error on IsJobGroupPaused - Group {groupName}");
        }

        public Task<bool> IsTriggerGroupPaused(string groupName, CancellationToken cancellationToken = new CancellationToken())
        {
            return DoWithLock(storage.IsTriggerGroupPausedAsync(groupName), $"Error on IsTriggerGroupPaused - Group {groupName}");
        }

        public Task PauseAll(CancellationToken cancellationToken = new CancellationToken())
        {
            return DoWithLock(storage.PauseAllTriggersAsync(), "Error on pausing all");
        }

        public Task PauseJob(JobKey jobKey, CancellationToken cancellationToken = new CancellationToken())
        {
            return DoWithLock(storage.PauseJobAsync(jobKey), $"Error on pausing job - {jobKey}");
        }

        public Task<IReadOnlyCollection<string>> PauseJobs(
            GroupMatcher<JobKey> matcher,
            CancellationToken cancellationToken = new CancellationToken())
        {
            return DoWithLock(storage.PauseJobsAsync(matcher), "Error on pausing jobs");
        }

        public Task PauseTrigger(TriggerKey triggerKey, CancellationToken cancellationToken = new CancellationToken())
        {
            return DoWithLock(storage.PauseTriggerAsync(triggerKey), $"Error on pausing trigger - {triggerKey}");
        }

        public Task<IReadOnlyCollection<string>> PauseTriggers(
            GroupMatcher<TriggerKey> matcher,
            CancellationToken cancellationToken = new CancellationToken())
        {
            return DoWithLock(storage.PauseTriggersAsync(matcher), "Error on pausing triggers");
        }

        public Task ReleaseAcquiredTrigger(IOperableTrigger trigger, CancellationToken cancellationToken = new CancellationToken())
        {
            return DoWithLock(storage.ReleaseAcquiredTriggerAsync(trigger), $"Error on releasing acquired trigger - {trigger}");
        }

        public Task<bool> RemoveCalendar(string calName, CancellationToken cancellationToken = new CancellationToken())
        {
            return DoWithLock(storage.RemoveCalendarAsync(calName), $"Error on removing calendar - {calName}");
        }

        public Task<bool> RemoveJob(JobKey jobKey, CancellationToken cancellationToken = new CancellationToken())
        {
            return DoWithLock(storage.RemoveJobAsync(jobKey), "Could not remove a job");
        }

        public async Task<bool> RemoveJobs(IReadOnlyCollection<JobKey> jobKeys, CancellationToken cancellationToken = new CancellationToken())
        {
            var removed = jobKeys.Count > 0;

            foreach (var jobKey in jobKeys)
            {
                removed &= await DoWithLock(storage.RemoveJobAsync(jobKey), $"Error on removing job - {jobKey}");
            }

            return removed;
        }

        public Task<bool> RemoveTrigger(TriggerKey triggerKey, CancellationToken cancellationToken = new CancellationToken())
        {
            return DoWithLock(storage.RemoveTriggerAsync(triggerKey), "Could not remove trigger");
        }

        public async Task<bool> RemoveTriggers(
            IReadOnlyCollection<TriggerKey> triggerKeys,
            CancellationToken cancellationToken = new CancellationToken())
        {
            var removed = triggerKeys.Count > 0;

            foreach (var triggerKey in triggerKeys)
            {
                removed &= await DoWithLock(storage.RemoveTriggerAsync(triggerKey), $"Error on removing trigger - {triggerKey}");
            }

            return removed;
        }

        public Task<bool> ReplaceTrigger(
            TriggerKey triggerKey,
            IOperableTrigger newTrigger,
            CancellationToken cancellationToken = new CancellationToken())
        {
            return DoWithLock(storage.ReplaceTriggerAsync(triggerKey, newTrigger), "Error on replacing trigger");
        }

        public Task ResumeAll(CancellationToken cancellationToken = new CancellationToken())
        {
            return DoWithLock(storage.PauseAllTriggersAsync(), "Error on pausing all");
        }

        public Task ResumeJob(JobKey jobKey, CancellationToken cancellationToken = new CancellationToken())
        {
            return DoWithLock(storage.ResumeJobAsync(jobKey), $"Error on resuming job - {jobKey}");
        }

        public Task<IReadOnlyCollection<string>> ResumeJobs(
            GroupMatcher<JobKey> matcher,
            CancellationToken cancellationToken = new CancellationToken())
        {
            return DoWithLock(storage.ResumeJobsAsync(matcher), "Error on resuming jobs");
        }

        public Task ResumeTrigger(TriggerKey triggerKey, CancellationToken cancellationToken = new CancellationToken())
        {
            return DoWithLock(storage.ResumeTriggerAsync(triggerKey), $"Error on resuming trigger - {triggerKey}");
        }

        public Task<IReadOnlyCollection<string>> ResumeTriggers(
            GroupMatcher<TriggerKey> matcher,
            CancellationToken cancellationToken = new CancellationToken())
        {
            return DoWithLock(storage.ResumeTriggersAsync(matcher), "Error on resume triggers");
        }

        public Task<ICalendar> RetrieveCalendar(string calName, CancellationToken cancellationToken = new CancellationToken())
        {
            return DoWithLock(storage.RetrieveCalendarAsync(calName), $"Error on retrieving calendar - {calName}");
        }

        public Task<IJobDetail> RetrieveJob(JobKey jobKey, CancellationToken cancellationToken = new CancellationToken())
        {
            return DoWithLock(storage.RetrieveJobAsync(jobKey), "Could not retrieve job");
        }

        public Task<IOperableTrigger> RetrieveTrigger(TriggerKey triggerKey, CancellationToken cancellationToken = new CancellationToken())
        {
            return DoWithLock(storage.RetrieveTriggerAsync(triggerKey), "could not retrieve trigger");
        }

        public Task SchedulerPaused(CancellationToken cancellationToken = new CancellationToken())
        {
            return Task.FromResult(0);
        }

        public Task SchedulerResumed(CancellationToken cancellationToken = new CancellationToken())
        {
            return Task.FromResult(0);
        }

        public Task SchedulerStarted(CancellationToken cancellationToken = new CancellationToken())
        {
            return Task.FromResult(0);
        }

        public Task Shutdown(CancellationToken cancellationToken = new CancellationToken())
        {
            Multiplexer.Dispose();
            return null;
        }

        public Task StoreCalendar(
            string name,
            ICalendar calendar,
            bool replaceExisting,
            bool updateTriggers,
            CancellationToken cancellationToken = new CancellationToken())
        {
            return DoWithLock(storage.StoreCalendarAsync(name, calendar, replaceExisting, updateTriggers), $"Error on store calendar - {name}");
        }

        public Task StoreJob(IJobDetail newJob, bool replaceExisting, CancellationToken cancellationToken = new CancellationToken())
        {
            return DoWithLock(storage.StoreJobAsync(newJob, replaceExisting), "Could not store job");
        }

        public async Task StoreJobAndTrigger(
            IJobDetail newJob,
            IOperableTrigger newTrigger,
            CancellationToken cancellationToken = new CancellationToken())
        {
            await DoWithLock(
                new[]
                    {
                        storage.StoreJobAsync(newJob, false),
                        storage.StoreTriggerAsync(newTrigger, false)
                    },
                "Could not store job/trigger");
        }

        public async Task StoreJobsAndTriggers(
            IReadOnlyDictionary<IJobDetail, IReadOnlyCollection<ITrigger>> triggersAndJobs,
            bool replace,
            CancellationToken cancellationToken = new CancellationToken())
        {
            foreach (var job in triggersAndJobs)
            {
                await DoWithLock(StoreJobsAndTriggers(job, replace), $"Could store job/trigger - {job.Key.Key.Name}");
            }
        }

        public Task StoreTrigger(IOperableTrigger newTrigger, bool replaceExisting, CancellationToken cancellationToken = new CancellationToken())
        {
            return DoWithLock(storage.StoreTriggerAsync(newTrigger, replaceExisting), "Could not store trigger");
        }

        public Task TriggeredJobComplete(
            IOperableTrigger trigger,
            IJobDetail jobDetail,
            SchedulerInstruction triggerInstCode,
            CancellationToken cancellationToken = new CancellationToken())
        {
            return DoWithLock(
                storage.TriggeredJobCompleteAsync(trigger, jobDetail, triggerInstCode),
                $"Error on triggered job complete - job:{jobDetail} - trigger:{trigger}");
        }

        public Task<IReadOnlyCollection<TriggerFiredResult>> TriggersFired(
            IReadOnlyCollection<IOperableTrigger> triggers,
            CancellationToken cancellationToken = new CancellationToken())
        {
            return DoWithLock(storage.TriggersFiredAsync(triggers), "Error on Triggers Fired");
        }

        private async Task DoWithLock(Task action, string errorMessage = "Job Storage error")
        {
            var sw = Stopwatch.StartNew();
            try
            {
                await action;
            }
            catch (ObjectAlreadyExistsException ex)
            {
                logger.Error("key exists", ex);
            }
            catch (Exception ex)
            {
                logger.Error(ex);
                throw new JobPersistenceException(errorMessage, ex);
            }
            finally
            {
                logger.Debug($"Execute done, Elapsed: {sw.Elapsed:g}");
            }
        }

        private async Task DoWithLock(Task[] action, string errorMessage = "Job Storage error")
        {
            var sw = Stopwatch.StartNew();
            try
            {
                foreach (var task in action)
                {
                    await task;
                }
            }
            catch (ObjectAlreadyExistsException ex)
            {
                logger.Error("key exists", ex);
            }
            catch (Exception ex)
            {
                logger.Error(ex);
                throw new JobPersistenceException(errorMessage, ex);
            }
            finally
            {
                logger.Debug($"Execute done, Elapsed: {sw.Elapsed:g}");
            }
        }

        private async Task<T> DoWithLock<T>(Task<T> fun, string errorMessage = "Job Storage error")
        {
            var sw = Stopwatch.StartNew();
            try
            {
                return await fun;
            }
            catch (ObjectAlreadyExistsException ex)
            {
                logger.Error("key exists", ex);
                throw;
            }
            catch (Exception ex)
            {
                logger.Error(ex);
                throw new JobPersistenceException(errorMessage, ex);
            }
            finally
            {
                logger.Debug($"Execute done, Elapsed: {sw.Elapsed:g}");
            }
        }

        private async Task StoreJobsAndTriggers(KeyValuePair<IJobDetail, IReadOnlyCollection<ITrigger>> job, bool replace)
        {
            await storage.StoreJobAsync(job.Key, replace);
            var tasks = job.Value.Select(trigger => storage.StoreTriggerAsync(trigger, replace));
            await Task.WhenAll(tasks);
        }
    }
}