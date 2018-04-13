namespace Quartz.RedisJobStore.Async.Enums
{
    public class JobStoreKeyEnum
    {
        public static readonly string Description = "Description";

        public static readonly string IsDurable = "Is_Durable";

        public static readonly string JobClass = "Job_Class_Name";

        public static readonly string RequestRecovery = "Request_Recovery";

        public static readonly string BlockTime = "Block_Time";

        public static readonly string BlockedBy = "Block_By";
    }
}