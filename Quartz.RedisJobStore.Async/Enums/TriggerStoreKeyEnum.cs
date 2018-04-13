namespace Quartz.RedisJobStore.Async.Enums
{
    public class TriggerStoreKeyEnum
    {
        public static readonly string CalendarName = "Calendar_Name";

        public static readonly string CronExpression = "CORN_Expression";

        public static readonly string Description = "Description";

        public static readonly string FireInstanceId = "Fire_Instance_Id";

        public static readonly string JobHash = "Job_Hash_Key";

        public static readonly string MisfireInstruction = "Misfire_Instruction";

        public static readonly string Priority = "Priority";

        public static readonly string RepeatCount = "Repeat_Count";

        public static readonly string RepeatInterval = "Repeat_Interval";

        public static readonly string TimesTriggered = "Times_Triggered";

        public static readonly string TimeZoneId = "Time_Zone_Id";

        public static readonly string TriggerType = "Trigger_Type";

        public static readonly string TriggerTypeCron = "CRON";

        public static readonly string TriggerTypeSimple = "Simple";

        public static readonly string NextFireTime = "Next_Fire_Time";

        public static readonly string PrevFireTime = "Prev_Fire_Time";

        public static readonly string StartTime = "Start_Time";

        public static readonly string EndTime = "End_Time";

        public static readonly string FinalFireTime = "Final_Fire_Time";
    }
}