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

        private readonly string prefix;

        public RedisKeySchema(string delimiter, string prefix)
        {
            this.delimiter = delimiter;
            this.prefix = $"{prefix}_";
        }

        public RedisKey LockKey => $"{prefix}Lock";

        public string BlockedJobs()
        {
            return $"{prefix}Blocked_Jobs";
        }

        public string BlockedJobsSet()
        {
            return $"{prefix}Blocked_Jobs";
        }

        public string CalendarHashKey(string name)
        {
            return $"{prefix}Calendar{delimiter}{name}";
        }

        public string CalendarsKey()
        {
            return $"{prefix}Calendars";
        }

        public string CalendarTriggersKey(string key)
        {
            return $"{prefix}Calendar_Triggers{delimiter}{key}";
        }

        public string JobBlockedKey(JobKey key)
        {
            return $"{prefix}Job_blocked{delimiter}{key.Group}{delimiter}{key.Name}";
        }

        public string JobDataMapHashKey(JobKey key)
        {
            return $"{prefix}Job_Data_Map{delimiter}{key.Group}{delimiter}{key.Name}";
        }

        public string JobGroup(string name)
        {
            return Split(name)[1];
        }

        public string JobGroupKey(string name)
        {
            return $"{prefix}Job_Group{delimiter}{name}";
        }

        public string JobGroupsKey()
        {
            return $"{prefix}Job_Groups";
        }

        public string JobHashKey(JobKey key)
        {
            return $"{prefix}{key.Group}{delimiter}{key.Name}";
        }

        public JobKey JobKey(string key)
        {
            var hashParts = Split(key);
            return new JobKey(hashParts[2], hashParts[1]);
        }

        public string JobsKey()
        {
            return $"{prefix}Jobs";
        }

        public string JobTriggersKey(JobKey key)
        {
            return $"{prefix}Job_Triggers{delimiter}{key.Group}{delimiter}{key.Name}";
        }

        public string LastTriggerReleaseTime()
        {
            return $"{prefix}last_triggers_release_time";
        }

        public string PausedJobGroupsKey()
        {
            return $"{prefix}Paused_Job_Groups";
        }

        public string PausedTriggerGroupsKey()
        {
            return $"{prefix}Paused_Trigger_Groups";
        }

        public string SetJobGroupsKey()
        {
            return $"{prefix}Job_Groups";
        }

        public string TriggerGroup(string group)
        {
            return Split(group)[1];
        }

        public string TriggerGroupSetKey(string group)
        {
            return $"{prefix}Trigger_Group{delimiter}{group}";
        }

        public string TriggerGroupsKey()
        {
            return $"{prefix}Trigger_Groups";
        }

        public string TriggerHashKey(TriggerKey key)
        {
            return $"{prefix}Trigger{delimiter}{key.Group}{delimiter}{key.Name}";
        }

        public TriggerKey TriggerKey(string key)
        {
            var hashParts = Split(key);
            return new TriggerKey(hashParts[2], hashParts[1]);
        }

        public string TriggerLockKey(TriggerKey key)
        {
            return $"{prefix}Trigger_Lock{delimiter}{key.Group}{delimiter}{key.Name}";
        }

        public string TriggersKey()
        {
            return $"{prefix}Triggers";
        }

        public string TriggerStateKey(TriggerStateEnum state)
        {
            return $"{prefix}{state.ToString()}_Triggers";
        }

        public string GetCalendarName(string name)
        {
            return Split(name)[1];
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
    }
}