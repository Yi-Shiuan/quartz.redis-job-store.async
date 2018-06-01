namespace Quartz.RedisJobStore.Async.Extensions
{
    using System;

    public static class DateTimeExtension
    {
        public static double ToUnixTimeMillieSeconds(this DateTime date)
        {
            var span = date - new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);
            return span.TotalMilliseconds;
        }
    }
}
