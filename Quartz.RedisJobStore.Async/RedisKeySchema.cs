namespace Quartz.RedisJobStore.Async
{
    #region

    using System;

    using Quartz.RedisJobStore.Async.Enums;

    using StackExchange.Redis;

    #endregion

    public class RedisKeySchema
    {
        private readonly string delimiter;

        public RedisKeySchema(string delimiter)
        {
            this.delimiter = delimiter;
        }

        public string RedisJobGroupKey()
        {
            return "Job_Groups";
        }

        public string RedisJobGroupKey(JobKey key)
        {
            return $"Job_Groups{delimiter}{key.Group}";
        }

        public string RedisJobGroupKey(string key)
        {
            return $"Job_Groups{delimiter}{key}";
        }

        public string RedisJobKey(JobKey key)
        {
            return $"Job{delimiter}{key.Group}{delimiter}{key.Name}";
        }

        public JobKey ToJobKey(string key)
        {
            var t = Split(key);
            return new JobKey(t[1], t[0]);
        }

        public string RedisJobKey()
        {
            return "Jobs";
        }
        
        public string JobStoreKey(JobKey key)
        {
            return $"{key.Group}{delimiter}{key.Name}";
        }

        public string RedisJobDataMap(JobKey jobKey)
        {
            return $"Job_DataMap{delimiter}{jobKey.Group}{delimiter}{jobKey.Name}";
        }
        
        
        
        
        
        
        
        

        public string BlockedJobs()
        {
            return "Blocked_Jobs";
        }

        public string BlockedJobsSet()
        {
            return "Blocked_Jobs";
        }

        public string CalendarHashKey(string name)
        {
            return $"Calendar{delimiter}{name}";
        }

        public string CalendarsKey()
        {
            return "Calendars";
        }

        public string CalendarTriggersKey(string key)
        {
            return $"Calendar_Triggers{delimiter}{key}";
        }

        public string GetCalendarName(string name)
        {
            return Split(name)[1];
        }

        public string JobBlockedKey(JobKey key)
        {
            return $"Job_blocked{delimiter}{key.Group}{delimiter}{key.Name}";
        }

        public string JobDataMapHashKey(JobKey key)
        {
            return $"Job_Data_Map{delimiter}{key.Group}{delimiter}{key.Name}";
        }

        public string JobGroup(string name)
        {
            return Split(name)[1];
        }

        public string JobGroupKey(string name)
        {
            return $"Job_Group{delimiter}{name}";
        }

        public string JobGroupsKey()
        {
            return "Job_Groups";
        }

        public string JobHashKey(JobKey key)
        {
            return $"Jobs{delimiter}{key.Group}{delimiter}{key.Name}";
        }

        public JobKey JobKey(string key)
        {
            var hashParts = Split(key);
            return new JobKey(hashParts[2], hashParts[1]);
        }

        public string JobsKey()
        {
            return "Jobs";
        }

        public string JobTriggersKey(JobKey key)
        {
            return $"Job_Triggers{delimiter}{key.Group}{delimiter}{key.Name}";
        }

        public string LastTriggerReleaseTime()
        {
            return "last_triggers_release_time";
        }

        public string PausedJobGroupsKey()
        {
            return "Paused_Job_Groups";
        }

        public string PausedTriggerGroupsKey()
        {
            return "Paused_Trigger_Groups";
        }

        public string[] Split(string input)
        {
            return input.Split(
                             new[]
                                 {
                                     delimiter
                                 },
                             StringSplitOptions.None);
        }

        public string TriggerGroup(string group)
        {
            return Split(group)[1];
        }

        public string TriggerGroupSetKey(string group)
        {
            return $"Trigger_Group{delimiter}{group}";
        }

        public string TriggerGroupsKey()
        {
            return "Trigger_Groups";
        }

        public string TriggerHashKey(TriggerKey key)
        {
            return $"Trigger{delimiter}{key.Group}{delimiter}{key.Name}";
        }

        public TriggerKey TriggerKey(string key)
        {
            var hashParts = Split(key);
            return new TriggerKey(hashParts[2], hashParts[1]);
        }

        public string TriggerLockKey(TriggerKey key)
        {
            return $"Trigger_Lock{delimiter}{key.Group}{delimiter}{key.Name}";
        }

        public string TriggersKey()
        {
            return "Triggers";
        }

        public string TriggerStateKey(TriggerState state)
        {
            return $"{state.ToString()}_Triggers";
        }
    }
}