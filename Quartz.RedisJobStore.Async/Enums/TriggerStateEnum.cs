namespace Quartz.RedisJobStore.Async.Enums
{
    public enum TriggerStateEnum
    {
        Waiting,

        Paused,

        Blocked,

        PausedBlocked,

        Acquired,

        Completed,

        Error
    }
}