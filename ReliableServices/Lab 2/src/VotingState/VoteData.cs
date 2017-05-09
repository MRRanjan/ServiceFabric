using Bond;
using System;

namespace VotingState
{
    // Defined as a Bond schema to assist with data versioning and serialization.
    // Using a structure and read only properties to make this an immutable entity.
    // This helps to ensure Reliable Collections are used properly.
    // See https://docs.microsoft.com/en-us/azure/service-fabric/service-fabric-      
    // work-with-reliable-collections
    [Bond.Schema]
    public struct VotingData
    {
        // Each field is attributed with an id that is unique. The field definition is never changed.
        // This is the name of the vote, which will also be the key of the reliable dictionary.
        [Bond.Id(0)]
        public string Name { get; private set; }

        // This is the current number of votes for this entity.
        [Bond.Id(10)]
        public int Count { get; private set; }

        // This is the maximum number of votes for this entity.
        [Bond.Id(20)]
        public int MaxCount { get; private set; }

        // DateTimeOffset is not a supported data type by Bond
        // This is the date and time of the last vote.
        [Bond.Id(30), Bond.Type(typeof(long))]
        public DateTimeOffset LastVote { get; private set; }

        // VotingData constructor.
        public VotingData(string name, int count, int maxCount, DateTimeOffset date)
        {
            this.Name = name;
            this.Count = count;
            this.MaxCount = maxCount;
            this.LastVote = date;
        }

        // Updates the count of a VotingData structure returning a new instance.
        public VotingData UpdateWith(int count)
        {
            int newMax = Math.Max(this.MaxCount, count);
            return new VotingData(this.Name, count, newMax, DateTimeOffset.Now);
        }
    }
}
