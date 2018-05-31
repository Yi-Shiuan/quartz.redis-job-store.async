using Quartz.RedisJobStore.Async.Enums;
using StackExchange.Redis;

namespace Quartz.RedisJobStore.Async
{
    public static class JobKeyExtension
    {
        public static HashEntry[] ToDataMapEntity(this JobDataMap jobDataMap)
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
        
        public static HashEntry[] ToJobEntity(this IJobDetail jobDetail)
        {
            return new[]
            {
                new HashEntry(JobStoreKey.JobClass, jobDetail.JobType.AssemblyQualifiedName),
                new HashEntry(JobStoreKey.Description, jobDetail.Description ?? string.Empty),
                new HashEntry(JobStoreKey.IsDurable, jobDetail.Durable),
                new HashEntry(JobStoreKey.RequestRecovery, jobDetail.RequestsRecovery),
                new HashEntry(JobStoreKey.BlockedBy, string.Empty),
                new HashEntry(JobStoreKey.BlockTime, string.Empty)
            };
        }
    }
}