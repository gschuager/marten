using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DaemonTests.TestingSupport;
using JasperFx.Core;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Marten;
using Marten.Events;
using Marten.Events.Daemon;
using Marten.Events.Daemon.Internals;
using Marten.Events.Projections;
using Marten.Storage;
using Marten.Testing.Harness;
using Npgsql;
using Shouldly;
using Weasel.Postgresql;
using Weasel.Postgresql.SqlGeneration;
using Xunit;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace DaemonTests.Bugs;

/// <summary>
/// Regression coverage for an async daemon page-loader race where concurrent appends can reserve
/// event sequence numbers ahead of committed event rows. The daemon can calculate a page ceiling
/// from the high-water mark, advance progression, and miss matching events that commit inside that
/// range after the page query ran.
/// </summary>
public class Bug_async_daemon_page_loader_skips_committed_events: DaemonContext
{
    private const int BatchSize = 50;
    private const int Concurrency = 24;
    private const int StreamsPerWorker = 80;
    private const int Attempts = 1;
    private const int SlowEventNumber = 0;
    private const int TailGapEventNumber = -1;
    private const int SlowTriggerLockClassId = 987654;
    private const int SlowTriggerLockObjectId = 321;

    public Bug_async_daemon_page_loader_skips_committed_events(ITestOutputHelper output): base(output)
    {
    }

    [Theory]
    [InlineData(EventAppendMode.QuickWithServerTimestamps)]
    [InlineData(EventAppendMode.Rich)]
    public async Task event_loader_does_not_advance_page_ceiling_over_trailing_or_interior_visibility_gap(
        EventAppendMode appendMode)
    {
        StoreOptions(opts =>
        {
            opts.Events.AppendMode = appendMode;
            opts.Events.EnableAdvancedAsyncTracking = true;
        });

        const int directLoaderBatchSize = 50;

        try
        {
            await theStore.Storage.ApplyAllConfiguredChangesToDatabaseAsync();
            await appendTrackedEvents(Enumerable.Range(1, 8));
            await installSlowEventInsertTrigger();

            var delayedAppend = appendTrackedEvents([TailGapEventNumber]);
            await waitForSlowTriggerToBeSleeping();

            await appendTrackedEvents(Enumerable.Range(9, 3));

            var highWater = await scalar<long>($"select coalesce(max(seq_id), 0) from {theStore.Events.DatabaseSchemaName}.mt_events");
            var reportedSequence = await scalar<long>($"select last_value from {theStore.Events.DatabaseSchemaName}.mt_events_sequence");

            var loader = new EventLoader(theStore, (MartenDatabase)theStore.Tenancy.Default.Database,
                new AsyncOptions { BatchSize = directLoaderBatchSize },
                [new EventTypeFilter(theStore.Options.EventGraph, [typeof(TrackedEvent)])]);

            var page = await loader.LoadAsync(new EventRequest
            {
                Floor = 0,
                HighWater = highWater,
                BatchSize = directLoaderBatchSize,
                ErrorOptions = new ErrorHandlingOptions(),
                Runtime = new NulloDaemonRuntime(),
                Name = new ShardName(nameof(TrackedView), "All", 1)
            }, default);

            await delayedAppend;

            var persistedSequences = await loadPersistedSequences();
            var loadedSequences = page.Select(x => x.Sequence).OrderBy(x => x).ToArray();
            var missingSequences = persistedSequences.Except(loadedSequences).OrderBy(x => x).ToArray();

            if (missingSequences.Length != 0 && page.Ceiling >= missingSequences[0])
            {
                throw new XunitException(new DirectLoaderFailureEvidence(
                    appendMode,
                    directLoaderBatchSize,
                    persistedSequences.Length,
                    loadedSequences.Length,
                    missingSequences,
                    0,
                    page.Ceiling,
                    page.Count,
                    highWater,
                    reportedSequence).ToString());
            }

            page.Ceiling.ShouldBeLessThan(missingSequences[0]);
        }
        finally
        {
            if (theStore != null)
            {
                await dropSlowEventInsertTrigger();
            }
        }
    }

    [Theory]
    [InlineData(EventAppendMode.QuickWithServerTimestamps)]
    [InlineData(EventAppendMode.Rich)]
    public async Task async_daemon_does_not_advance_past_committed_matching_events(EventAppendMode appendMode)
    {
        StoreOptions(opts =>
        {
            opts.Events.AppendMode = appendMode;
            opts.Events.EnableAdvancedAsyncTracking = true;
            opts.Projections.Add(new TrackedViewProjection { Options = { BatchSize = BatchSize } }, ProjectionLifecycle.Async);
        });

        FailureEvidence lastEvidence = null;

        try
        {
            for (var attempt = 1; attempt <= Attempts; attempt++)
            {
                await theStore.Advanced.Clean.DeleteAllDocumentsAsync();
                await theStore.Advanced.Clean.DeleteAllEventDataAsync();
                await resetProjectionProgression();
                await installSlowEventInsertTrigger();

                using (var daemon = await StartDaemon())
                {
                    var slowAppend = appendSlowTrackedEvent();
                    await Task.Delay(100);

                    var fastAppends = Enumerable.Range(0, Concurrency)
                        .Select(worker => appendTrackedEvents(worker, attempt))
                        .ToArray();

                    await Task.WhenAll(fastAppends);
                    await daemon.WaitForNonStaleData(60.Seconds());

                    await slowAppend;
                    await daemon.WaitForNonStaleData(15.Seconds());
                }

                lastEvidence = await collectEvidence(appendMode, attempt);
                if (lastEvidence.MissingSequences.Length != 0)
                {
                    throw new XunitException(lastEvidence.ToString());
                }
            }
        }
        finally
        {
            if (theStore != null)
            {
                await dropSlowEventInsertTrigger();
            }
        }

        if (lastEvidence == null || lastEvidence.PersistedCount == 0)
        {
            throw new XunitException($"No committed {nameof(TrackedEvent)} rows were found for append mode {appendMode}");
        }
    }

    private async Task appendTrackedEvents(int worker, int attempt)
    {
        var random = new Random(HashCode.Combine(worker, attempt));

        for (var i = 0; i < StreamsPerWorker; i++)
        {
            await using var session = theStore.LightweightSession();

            var aggregateId = Guid.NewGuid();
            session.Events.StartStream<TrackedView>(aggregateId,
                new TrackedEvent(aggregateId, worker * StreamsPerWorker + i + 1));

            await Task.Delay(random.Next(0, 8));
            await session.SaveChangesAsync();
            await Task.Delay(random.Next(0, 4));
        }
    }

    private async Task appendSlowTrackedEvent()
    {
        await using var session = theStore.LightweightSession();

        var aggregateId = Guid.NewGuid();
        session.Events.StartStream<TrackedView>(aggregateId, new TrackedEvent(aggregateId, SlowEventNumber));

        await session.SaveChangesAsync();
    }

    private async Task appendTrackedEvents(IEnumerable<int> numbers)
    {
        await using var session = theStore.LightweightSession();

        foreach (var number in numbers)
        {
            var aggregateId = Guid.NewGuid();
            session.Events.StartStream<TrackedView>(aggregateId, new TrackedEvent(aggregateId, number));
        }

        await session.SaveChangesAsync();
    }

    private async Task installSlowEventInsertTrigger()
    {
        var schema = theStore.Events.DatabaseSchemaName;

        // Hold one normal append after sequence assignment to make the concurrent-append gap deterministic.
        await executeSql($$"""
                         CREATE OR REPLACE FUNCTION {{schema}}.sleep_tracked_event_insert() RETURNS trigger LANGUAGE plpgsql AS $function$
                         BEGIN
                             IF NEW.data ->> 'Number' IN ('0', '-1') OR NEW.data ->> 'number' IN ('0', '-1') THEN
                                 PERFORM pg_advisory_lock({{SlowTriggerLockClassId}}, {{SlowTriggerLockObjectId}});
                                 BEGIN
                                     PERFORM pg_sleep(2);
                                 EXCEPTION WHEN OTHERS THEN
                                     PERFORM pg_advisory_unlock({{SlowTriggerLockClassId}}, {{SlowTriggerLockObjectId}});
                                     RAISE;
                                 END;
                                 PERFORM pg_advisory_unlock({{SlowTriggerLockClassId}}, {{SlowTriggerLockObjectId}});
                             END IF;

                             RETURN NEW;
                         END;
                         $function$;

                         DROP TRIGGER IF EXISTS sleep_tracked_event_insert ON {{schema}}.mt_events;

                         CREATE TRIGGER sleep_tracked_event_insert
                         AFTER INSERT ON {{schema}}.mt_events
                         FOR EACH ROW
                         WHEN (NEW.data ->> 'Number' IN ('0', '-1') OR NEW.data ->> 'number' IN ('0', '-1'))
                         EXECUTE FUNCTION {{schema}}.sleep_tracked_event_insert();
                         """);
    }

    private async Task waitForSlowTriggerToBeSleeping()
    {
        var started = DateTimeOffset.UtcNow;
        while (DateTimeOffset.UtcNow - started < 5.Seconds())
        {
            var lockCount = await scalar<long>($"select count(*) from pg_locks where locktype = 'advisory' and classid = {SlowTriggerLockClassId} and objid = {SlowTriggerLockObjectId}");
            if (lockCount > 0)
            {
                return;
            }

            await Task.Delay(25);
        }

        throw new TimeoutException("Timed out waiting for the slow event insert trigger to reach pg_sleep");
    }

    private Task dropSlowEventInsertTrigger()
    {
        var schema = theStore.Events.DatabaseSchemaName;

        return executeSql($$"""
                          DROP TRIGGER IF EXISTS sleep_tracked_event_insert ON {{schema}}.mt_events;
                          DROP FUNCTION IF EXISTS {{schema}}.sleep_tracked_event_insert();
                          """);
    }

    private Task resetProjectionProgression()
    {
        return executeSql($"delete from {theStore.Events.DatabaseSchemaName}.mt_event_progression");
    }

    private async Task<FailureEvidence> collectEvidence(EventAppendMode appendMode, int attempt)
    {
        await using var query = theStore.QuerySession();
        var views = await query.Query<TrackedView>().ToListAsync();
        var projectedSequences = views.SelectMany(x => x.Sequences).OrderBy(x => x).ToArray();
        var persistedSequences = await loadPersistedSequences();
        var missingSequences = persistedSequences.Except(projectedSequences).OrderBy(x => x).ToArray();
        var progression = await loadProgression();
        var highWaterSkipCount = await scalar<long>($"select count(*) from {theStore.Events.DatabaseSchemaName}.mt_high_water_skips");
        var deadLetterCount = await query.Query<DeadLetterEvent>().CountAsync();

        return new FailureEvidence(
            appendMode,
            attempt,
            BatchSize,
            Concurrency,
            persistedSequences.Length,
            projectedSequences.Length,
            missingSequences,
            progression,
            highWaterSkipCount,
            deadLetterCount);
    }

    private async Task<long[]> loadPersistedSequences()
    {
        var sequences = new List<long>();
        await using var conn = new NpgsqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();

        await using var reader = await conn.CreateCommand($"select seq_id from {theStore.Events.DatabaseSchemaName}.mt_events order by seq_id")
            .ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            sequences.Add(reader.GetInt64(0));
        }

        return sequences.ToArray();
    }

    private async Task<long> loadProgression()
    {
        return await scalar<long>(
            $"select coalesce(max(last_seq_id), 0) from {theStore.Events.DatabaseSchemaName}.mt_event_progression where name <> 'HighWaterMark'");
    }

    private async Task<T> scalar<T>(string sql)
    {
        await using var conn = new NpgsqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();

        var value = await conn.CreateCommand(sql).ExecuteScalarAsync();
        return (T)value!;
    }

    private async Task executeSql(string sql)
    {
        await using var conn = new NpgsqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();

        await conn.CreateCommand(sql).ExecuteNonQueryAsync();
    }

    public record TrackedEvent(Guid AggregateId, int Number);

    public class TrackedView
    {
        public Guid Id { get; set; }

        public List<int> Numbers { get; set; } = new();

        public List<long> Sequences { get; set; } = new();
    }

    public partial class TrackedViewProjection: MultiStreamProjection<TrackedView, Guid>
    {
        public TrackedViewProjection()
        {
            Identity<TrackedEvent>(x => x.AggregateId);
        }

        public void Apply(TrackedView view, IEvent<TrackedEvent> @event)
        {
            view.Numbers.Add(@event.Data.Number);
            view.Sequences.Add(@event.Sequence);
        }
    }

    private record FailureEvidence(
        EventAppendMode AppendMode,
        int Attempt,
        int BatchSize,
        int Concurrency,
        int PersistedCount,
        int ProjectedCount,
        long[] MissingSequences,
        long ProjectionProgression,
        long HighWaterSkipCount,
        long DeadLetterCount)
    {
        public override string ToString()
        {
            return $"Async daemon advanced past committed {nameof(TrackedEvent)} rows without projecting them. " +
                   $"appendMode={AppendMode}; attempt={Attempt}; batchSize={BatchSize}; concurrency={Concurrency}; " +
                   $"persisted={PersistedCount}; projected={ProjectedCount}; missingSequences=[{string.Join(", ", MissingSequences.Take(50))}]" +
                   $"{(MissingSequences.Length > 50 ? $", ... ({MissingSequences.Length} total)" : string.Empty)}; " +
                   $"projectionProgression={ProjectionProgression}; highWaterSkips={HighWaterSkipCount}; " +
                   $"deadLetters={DeadLetterCount}";
        }
    }

    private record DirectLoaderFailureEvidence(
        EventAppendMode AppendMode,
        int BatchSize,
        int PersistedCount,
        int ProjectedCount,
        long[] MissingSequences,
        long PageFloor,
        long PageCeiling,
        int PageSize,
        long HighWater,
        long ReportedSequence)
    {
        public override string ToString()
        {
            return $"Event loader advanced a non-empty page over a trailing/interior matching-event visibility gap. " +
                   $"appendMode={AppendMode}; batchSize={BatchSize}; persisted={PersistedCount}; projected={ProjectedCount}; " +
                   $"missingSequences=[{string.Join(", ", MissingSequences.Take(50))}]" +
                   $"{(MissingSequences.Length > 50 ? $", ... ({MissingSequences.Length} total)" : string.Empty)}; " +
                   $"page.floor={PageFloor}; page.ceiling={PageCeiling}; page.size={PageSize}; highWater={HighWater}; " +
                   $"reportedSequence={ReportedSequence}; highWaterExceededReportedSequence={HighWater > ReportedSequence}";
        }
    }
}
