namespace Quartz.RedisJobStore.Async
{
    #region

    using System;
    using Quartz.RedisJobStore.Async.Enums;

    #endregion

    public class RedisKeySchema
    {
        private readonly string delimiter;

        public RedisKeySchema(string delimiter)
        {
            this.delimiter = delimiter;
        }

        public string JobGroupStoreKey(JobKey key)
        {
            return key.Group;
        }

        public string JobStoreKey(JobKey key)
        {
            return $"{key.Group}{delimiter}{key.Name}";
        }

        public string RedisCalendarKey(string calendarName)
        {
            return $"Calendar{delimiter}{calendarName}";
        }
        
        public string RedisCalendarKey()
        {
            return $"Calendars";
        }

        public string RedisJobDataMap(JobKey jobKey)
        {
            return $"Job_DataMap{delimiter}{jobKey.Group}{delimiter}{jobKey.Name}";
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

        public string RedisJobKey()
        {
            return "Jobs";
        }

        public string RedisTriggerGroupKey(TriggerKey key)
        {
            return $"Trigger_Groups{delimiter}{key.Group}";
        }

        public string RedisTriggerGroupKey(string key)
        {
            return $"Trigger_Groups{delimiter}{key}";
        }

        public string RedisTriggerGroupKey()
        {
            return "Trigger_Groups";
        }

        public string RedisTriggerJobKey(JobKey key)
        {
            return $"Job_Triggers{delimiter}{key.Group}{delimiter}{key.Name}";
        }

        public string RedisTriggerKey()
        {
            return "Triggers";
        }

        public string RedisTriggerKey(TriggerKey key)
        {
            return $"Trigger{delimiter}{key.Group}{delimiter}{key.Name}";
        }

        public string RedisTriggerStateKey(TriggerRedisState state)
        {
            return $"Trigger_State{delimiter}{state}";
        }

        public JobKey ToJobKey(string key)
        {
            var t = Split(key);
            return new JobKey(t[1], t[0]);
        }

        public TriggerKey ToTriggerKey(string key)
        {
            var t = Split(key);
            return new TriggerKey(t[1], t[0]);
        }

        public string TriggerGroupStoreKey(TriggerKey key)
        {
            return key.Group;
        }

        public string TriggerStoreKey(TriggerKey key)
        {
            return $"{key.Group}{delimiter}{key.Name}";
        }

        public string RedisTriggerGroupStateKey(TriggerRedisState state)
        {
            return $"Trigger_Group_State{delimiter}{state}";
        }

        private string[] Split(string input)
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