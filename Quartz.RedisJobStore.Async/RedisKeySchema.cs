namespace Quartz.RedisJobStore.Async
{
    #region

    using System;

    using Quartz.RedisJobStore.Async.Enums;

    using StackExchange.Redis;

    using TriggerState = Quartz.TriggerState;

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
        
        public string TriggerStoreKey(TriggerKey key)
        {
            return $"{key.Group}{delimiter}{key.Name}";
        }

        public string RedisTriggerGroupKey(TriggerKey key)
        {
            return $"Trigger{delimiter}{key.Group}";
        }

        public string RedisTriggerGroupKey()
        {
            return "Trigger_Groups";
        }

        public string RedisTriggerKey()
        {
            return "Triggers";
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

        public string RedisTriggerKey(TriggerKey key)
        {
            return $"Trigger{delimiter}{key.Group}{delimiter}{key.Name}";
        }

        public string RedisTriggerJobKey(JobKey key)
        {
            return $"Job_Triggers{delimiter}{key.Group}{delimiter}{key.Name}";
        }

        public string RedisCalendarKey(string calendarName)
        {
            return $"Calendar{delimiter}{calendarName}";
        }

        public string RedisTriggerStateKey(TriggerRedisState state)
        {
            return $"Trigger_State{delimiter}{state}";
        }
    }
}