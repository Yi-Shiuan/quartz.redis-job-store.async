namespace Quartz.RedisJobStore.Async.Extensions
{
    #region

    using Quartz.RedisJobStore.Async.Enums;

    using StackExchange.Redis;

    #endregion

    public static class JobKeyExtension
    {
        public static HashEntry[] ToStoreEntity(this JobDataMap jobDataMap)
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

        public static HashEntry[] ToStoreEntries(this IJobDetail detail)
        {
            return new[]
                       {
                           new HashEntry(JobStoreKey.JobClass, detail.JobType.AssemblyQualifiedName),
                           new HashEntry(JobStoreKey.Description, detail.Description ?? string.Empty),
                           new HashEntry(JobStoreKey.IsDurable, detail.Durable),
                           new HashEntry(JobStoreKey.RequestRecovery, detail.RequestsRecovery),
                           new HashEntry(JobStoreKey.BlockedBy, string.Empty),
                           new HashEntry(JobStoreKey.BlockTime, string.Empty)
                       };
        }
    }
}