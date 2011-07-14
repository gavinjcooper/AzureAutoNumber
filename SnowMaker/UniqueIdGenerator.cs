﻿using System;
using System.Collections.Generic;
using System.Threading;

namespace SnowMaker
{
    public class UniqueIdGenerator : IUniqueIdGenerator
    {
        readonly int rangeSize;
        readonly int maxWriteAttempts;
        readonly IOptimisticDataStore optimisticDataStore;

        readonly IDictionary<string, ScopeState> states = new Dictionary<string, ScopeState>();
        readonly object statesLock = new object();

        public UniqueIdGenerator(
            IOptimisticDataStore optimisticDataStore,
            int rangeSize = 100,
            int maxWriteAttempts = 25)
        {
            this.rangeSize = rangeSize;
            this.maxWriteAttempts = maxWriteAttempts;
            this.optimisticDataStore = optimisticDataStore;
        }

        public long NextId(string scopeName)
        {
            var state = GetScopeState(scopeName);

            lock (state.IdGenerationLock)
            {
                if (state.LastId == state.UpperLimit)
                    UpdateFromSyncStore(scopeName, state);

                return Interlocked.Increment(ref state.LastId);
            }
        }

        ScopeState GetScopeState(string scopeName)
        {
            return states.GetValue(
                scopeName,
                statesLock,
                () => new ScopeState());
        }

        void UpdateFromSyncStore(string scopeName, ScopeState state)
        {
            var writesAttempted = 0;

            while (writesAttempted < maxWriteAttempts)
            {
                var data = optimisticDataStore.GetData(scopeName);

                if (!Int64.TryParse(data, out state.LastId))
                {
                    throw new Exception(string.Format(
                       "Data '{0}' in storage was corrupt and could not be parsed as an Int64"
                       , data));
                }

                state.UpperLimit = state.LastId + rangeSize;

                if (optimisticDataStore.TryOptimisticWrite(scopeName, state.UpperLimit.ToString()))
                {
                    // update succeeded
                    return;
                }

                writesAttempted++;
            }

            throw new Exception(string.Format(
                "Failed to update the OptimisticSyncStore after {0} attempts",
                writesAttempted));
        }
    }
}
