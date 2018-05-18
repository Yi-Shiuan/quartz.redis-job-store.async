namespace Quartz.RedisJobStore.Async.Enums
{
    public enum TriggerState
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