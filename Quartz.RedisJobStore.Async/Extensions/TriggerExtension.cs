namespace Quartz.RedisJobStore.Async.Extensions
{
    #region

    using System;
    using System.Collections.Generic;
    using System.Globalization;

    using Quartz.RedisJobStore.Async.Enums;
    using Quartz.Spi;

    using StackExchange.Redis;

    #endregion

    public static class TriggerExtension
    {
        public static HashEntry[] ToTriggerStoreEntries(this ITrigger trigger, string jobKey)
        {
            var operable = (IOperableTrigger)trigger;
            if (operable == null)
            {
                throw new InvalidCastException("trigger needs to be IOperable");
            }

            var entries = new List<HashEntry>
                              {
                                  new HashEntry(TriggerStoreKey.JobHash, operable.JobKey == null ? string.Empty : jobKey),
                                  new HashEntry(TriggerStoreKey.Description, operable.Description ?? string.Empty),
                                  new HashEntry(
                                      TriggerStoreKey.NextFireTime,
                                      operable.GetNextFireTimeUtc().HasValue
                                          ? operable.GetNextFireTimeUtc()
                                                    .GetValueOrDefault()
                                                    .DateTime.ToUnixTimeMillieSeconds()
                                                    .ToString(CultureInfo.InvariantCulture)
                                          : string.Empty),
                                  new HashEntry(
                                      TriggerStoreKey.PrevFireTime,
                                      operable.GetPreviousFireTimeUtc().HasValue
                                          ? operable.GetPreviousFireTimeUtc()
                                                    .GetValueOrDefault()
                                                    .DateTime.ToUnixTimeMillieSeconds()
                                                    .ToString(CultureInfo.InvariantCulture)
                                          : string.Empty),
                                  new HashEntry(TriggerStoreKey.Priority, operable.Priority),
                                  new HashEntry(TriggerStoreKey.StartTime, operable.StartTimeUtc.DateTime.ToUnixTimeMillieSeconds()),
                                  new HashEntry(
                                      TriggerStoreKey.EndTime,
                                      operable.EndTimeUtc.HasValue
                                          ? operable.EndTimeUtc.Value.DateTime.ToUnixTimeMillieSeconds()
                                                    .ToString(CultureInfo.InvariantCulture)
                                          : string.Empty),
                                  new HashEntry(
                                      TriggerStoreKey.FinalFireTime,
                                      operable.FinalFireTimeUtc.HasValue
                                          ? operable.FinalFireTimeUtc.Value.DateTime.ToUnixTimeMillieSeconds()
                                                    .ToString(CultureInfo.InvariantCulture)
                                          : string.Empty),
                                  new HashEntry(TriggerStoreKey.FireInstanceId, operable.FireInstanceId ?? string.Empty),
                                  new HashEntry(TriggerStoreKey.MisfireInstruction, operable.MisfireInstruction),
                                  new HashEntry(TriggerStoreKey.CalendarName, operable.CalendarName ?? string.Empty)
                              };

            switch (operable)
            {
                case ISimpleTrigger _:
                    entries.Add(new HashEntry(TriggerStoreKey.TriggerType, TriggerStoreKey.TriggerTypeSimple));
                    entries.Add(new HashEntry(TriggerStoreKey.RepeatCount, ((ISimpleTrigger)operable).RepeatCount));
                    entries.Add(new HashEntry(TriggerStoreKey.RepeatInterval, ((ISimpleTrigger)operable).RepeatInterval.ToString()));
                    entries.Add(new HashEntry(TriggerStoreKey.TimesTriggered, ((ISimpleTrigger)operable).TimesTriggered));
                    break;
                case ICronTrigger _:
                    entries.Add(new HashEntry(TriggerStoreKey.TriggerType, TriggerStoreKey.TriggerTypeCron));
                    entries.Add(new HashEntry(TriggerStoreKey.CronExpression, ((ICronTrigger)operable).CronExpressionString));
                    entries.Add(new HashEntry(TriggerStoreKey.TimeZoneId, ((ICronTrigger)operable).TimeZone.Id));
                    break;
            }

            return entries.ToArray();
        }
    }
}