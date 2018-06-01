namespace Quartz.RedisJobStore.Async.Enums
{
    public enum TriggerRedisState
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