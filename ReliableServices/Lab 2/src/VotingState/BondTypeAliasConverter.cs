using System;

namespace VotingState
{
    /// <summary>
    /// This is needed to support DateTimeOffset use in Bond schemas.
    /// It converts the DateTimeOffset type to a long value and 
    /// long values into DateTimeOffsets. See
    /// https://microsoft.github.io/bond/manual/bond_cs.html#converter
    /// </summary>
    public static class BondTypeAliasConverter
    {
        /// <summary>
        /// Converts a DateTimeOffset value to a long.
        /// </summary>
        /// <param name="value">DateTimeOffset value to convert.</param>
        /// <returns>Long integer value containing the number of ticks for
        /// the DateTimeOffset in UTC.</returns>
        public static long Convert(DateTimeOffset value, long unused)
        {
            return value.UtcTicks;
        }

        /// <summary>
        /// Converts a long value to a DateTimeOffset value.
        /// </summary>
        /// <param name="value">Long value to convert. This value must
        /// be in UTC.</param>
        /// <param name="unused"></param>
        /// <returns>DateTimeOffset</returns>
        public static DateTimeOffset Convert(long value, DateTimeOffset unused)
        {
            return new DateTimeOffset(value, TimeSpan.Zero);
        }
    }
}
