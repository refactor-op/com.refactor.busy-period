using System;
using System.Collections.Generic;
using System.Threading;
using Cysharp.Threading.Tasks;

namespace Refactor.BusyPeriod
{
    /// <summary>Exposes the completion of one work item and of the busy period that admitted it.</summary>
    public readonly struct BusyPeriodResponse
    {
        internal BusyPeriodResponse(UniTask work, UniTask period)
        {
            Work = work;
            Period = period;
        }

        /// <summary>Completes when this work item settles. This task has a single consumer.</summary>
        public UniTask Work { get; }

        /// <summary>Completes when the containing busy period settles. This task supports multiple consumers.</summary>
        public UniTask Period { get; }
    }

    /// <summary>Creates strands while inferring the caller-owned state and submitted work types.</summary>
    public static class BusyPeriod
    {
        /// <summary>
        /// Creates a strand where work arriving during deactivate joins the current period and interrupts that deactivate.
        /// </summary>
        /// <remarks>
        /// The deactivate callback must settle after its cancellation token is canceled. Cancellation caused by joining work
        /// is control flow; any other deactivate or cancellation-callback failure fails the period.
        /// </remarks>
        public static BusyPeriodStrand<TWork> Interruptible<TState, TWork>(
            TState state,
            Func<TState, UniTask> activate,
            Func<TState, TWork, UniTask> invoke,
            Func<TState, CancellationToken, UniTask> deactivate) =>
            new InterruptibleBusyPeriodStrand<TState, TWork>(state, activate, invoke, deactivate);

        /// <summary>
        /// Creates a strand where the current period closes before deactivate and work arriving during deactivate starts the next
        /// period.
        /// </summary>
        public static BusyPeriodStrand<TWork> Uninterruptible<TState, TWork>(
            TState state,
            Func<TState, UniTask> activate,
            Func<TState, TWork, UniTask> invoke,
            Func<TState, UniTask> deactivate) =>
            new UninterruptibleBusyPeriodStrand<TState, TWork>(state, activate, invoke, deactivate);
    }

    /// <summary>Serially dispatches work inside activate/deactivate-bounded busy periods.</summary>
    /// <remarks>
    /// Dispatch is thread-safe and imposes no asynchronous boundary: callbacks that complete synchronously may run
    /// before Dispatch returns. Activate failure fails the period and all work still waiting in it; work failure affects
    /// only that work; deactivate failure fails the period and any work that joined while deactivate was in progress.
    /// </remarks>
    public abstract class BusyPeriodStrand<TWork>
    {
        internal BusyPeriodStrand()
        {
        }

        /// <summary>Atomically admits work and returns its work and period completions.</summary>
        public abstract BusyPeriodResponse Dispatch(TWork work);
    }

    internal abstract class BusyPeriodStrand<TState, TWork> : BusyPeriodStrand<TWork>
    {
        private readonly TState _state;
        private readonly Func<TState, UniTask> _activate;
        private readonly Func<TState, TWork, UniTask> _invoke;
        private readonly object _gate = new();
        private Period _currentPeriod;
        private UniTask _previousPeriodCompletion = UniTask.CompletedTask;

        protected BusyPeriodStrand(TState state, Func<TState, UniTask> activate, Func<TState, TWork, UniTask> invoke)
        {
            _state = state;
            _activate = activate ?? throw new ArgumentNullException(nameof(activate));
            _invoke = invoke ?? throw new ArgumentNullException(nameof(invoke));
        }

        protected TState State => _state;

        public sealed override BusyPeriodResponse Dispatch(TWork work)
        {
            var request = new Request(work);
            Period period;
            InterruptibleDeactivateAttempt interruption;
            var start = false;

            lock (_gate)
            {
                period = _currentPeriod;
                if (period == null)
                {
                    period = new Period(_previousPeriodCompletion, request);
                    _currentPeriod = period;
                    start = true;
                }
                else
                {
                    period.Enqueue(request);
                }

                interruption = period.CurrentDeactivate;
            }

            // Publish outside the gate: resuming deactivate may invoke user cancellation callbacks synchronously.
            interruption?.Interrupt();

            if (start) Run(period).Forget();
            return new BusyPeriodResponse(request.Completion, period.Completion);
        }

        protected abstract InterruptibleDeactivateAttempt BeginDeactivate(Period period);
        protected abstract UniTask Deactivate(InterruptibleDeactivateAttempt interruption);

        protected void Close(Period period)
        {
            if (!ReferenceEquals(_currentPeriod, period)) return;
            _currentPeriod = null;
            _previousPeriodCompletion = period.Completion;
        }

        private async UniTask Run(Period period)
        {
            try
            {
                await period.PreviousCompletion;
            }
            catch
            {
                // ignored
            }

            try
            {
                await _activate(_state);
            }
            catch (Exception failure)
            {
                Request[] waiting;
                lock (_gate)
                {
                    Close(period);
                    waiting = period.TakeAll();
                }

                for (var index = 0; index < waiting.Length; index++)
                {
                    if (failure is OperationCanceledException canceled)
                        waiting[index].CompletionSource.TrySetCanceled(canceled.CancellationToken);
                    else
                        waiting[index].CompletionSource.TrySetException(failure);
                }

                if (failure is OperationCanceledException periodCanceled)
                    period.CompletionSource.TrySetCanceled(periodCanceled.CancellationToken);
                else
                    period.CompletionSource.TrySetException(failure);
                return;
            }

            while (true)
            {
                Request request;
                InterruptibleDeactivateAttempt deactivate;
                bool hasRequest;
                lock (_gate)
                {
                    hasRequest = period.TryDequeue(out request);
                    deactivate = hasRequest ? null : BeginDeactivate(period);
                }

                if (hasRequest)
                {
                    try
                    {
                        await _invoke(_state, request.Work);
                        request.CompletionSource.TrySetResult();
                    }
                    catch (OperationCanceledException canceled)
                    {
                        request.CompletionSource.TrySetCanceled(canceled.CancellationToken);
                    }
                    catch (Exception failure)
                    {
                        request.CompletionSource.TrySetException(failure);
                    }

                    continue;
                }

                Exception deactivateFailure = null;
                try
                {
                    await Deactivate(deactivate);
                }
                catch (Exception failure)
                {
                    deactivateFailure = failure;
                }

                bool finished;
                lock (_gate)
                {
                    finished = deactivate == null || period.TryFinishInterruptibleDeactivate(deactivate);
                    if (finished) Close(period);
                }

                var joinedCancellation = deactivateFailure is OperationCanceledException deactivateCancellation
                    && deactivate != null
                    && !finished
                    && deactivateCancellation.CancellationToken == deactivate.Cancellation;

                deactivate?.Dispose();

                var failureToPublish = joinedCancellation ? null : deactivateFailure;

                if (failureToPublish != null)
                {
                    Request[] waiting;
                    lock (_gate)
                    {
                        Close(period);
                        waiting = period.TakeAll();
                    }

                    for (var index = 0; index < waiting.Length; index++)
                    {
                        if (failureToPublish is OperationCanceledException canceled)
                            waiting[index].CompletionSource.TrySetCanceled(canceled.CancellationToken);
                        else
                            waiting[index].CompletionSource.TrySetException(failureToPublish);
                    }

                    if (failureToPublish is OperationCanceledException periodCanceled)
                        period.CompletionSource.TrySetCanceled(periodCanceled.CancellationToken);
                    else
                        period.CompletionSource.TrySetException(failureToPublish);
                    return;
                }

                if (finished)
                {
                    period.CompletionSource.TrySetResult();
                    return;
                }

                try
                {
                    await _activate(_state);
                }
                catch (Exception failure)
                {
                    Request[] waiting;
                    lock (_gate)
                    {
                        Close(period);
                        waiting = period.TakeAll();
                    }

                    for (var index = 0; index < waiting.Length; index++)
                    {
                        if (failure is OperationCanceledException canceled)
                            waiting[index].CompletionSource.TrySetCanceled(canceled.CancellationToken);
                        else
                            waiting[index].CompletionSource.TrySetException(failure);
                    }

                    if (failure is OperationCanceledException periodCanceled)
                        period.CompletionSource.TrySetCanceled(periodCanceled.CancellationToken);
                    else
                        period.CompletionSource.TrySetException(failure);
                    return;
                }
            }
        }

        protected sealed class InterruptibleDeactivateAttempt : IDisposable
        {
            private readonly CancellationTokenSource _cancellation = new CancellationTokenSource();
            private readonly UniTaskCompletionSource<bool> _interruption = new UniTaskCompletionSource<bool>();

            internal CancellationToken Cancellation => _cancellation.Token;
            internal UniTask<bool> Interruption => _interruption.Task;
            internal void Interrupt() => _interruption.TrySetResult(true);
            internal void Cancel() => _cancellation.Cancel();

            public void Dispose() => _cancellation.Dispose();
        }

        protected readonly struct Request
        {
            internal Request(TWork work)
            {
                Work = work;
                CompletionSource = AutoResetUniTaskCompletionSource.Create();
                Completion = CompletionSource.Task;
            }

            internal TWork Work { get; }
            internal AutoResetUniTaskCompletionSource CompletionSource { get; }
            internal UniTask Completion { get; }
        }

        protected sealed class Period
        {
            private readonly Queue<Request> _waiting = new();
            private InterruptibleDeactivateAttempt _deactivate;

            internal Period(UniTask previousCompletion, Request first)
            {
                PreviousCompletion = previousCompletion;
                CompletionSource = new UniTaskCompletionSource();
                Completion = CompletionSource.Task;
                _waiting.Enqueue(first);
            }

            internal UniTask PreviousCompletion { get; }
            internal UniTaskCompletionSource CompletionSource { get; }
            internal UniTask Completion { get; }
            internal InterruptibleDeactivateAttempt CurrentDeactivate => _deactivate;
            internal void Enqueue(Request request) => _waiting.Enqueue(request);
            internal bool TryDequeue(out Request request) => _waiting.TryDequeue(out request);

            internal InterruptibleDeactivateAttempt StartInterruptibleDeactivate()
            {
                _deactivate = new InterruptibleDeactivateAttempt();
                return _deactivate;
            }

            internal bool TryFinishInterruptibleDeactivate(InterruptibleDeactivateAttempt deactivate)
            {
                if (!ReferenceEquals(_deactivate, deactivate)) return false;
                _deactivate = null;
                return _waiting.Count == 0;
            }

            internal Request[] TakeAll()
            {
                var waiting = _waiting.ToArray();
                _waiting.Clear();
                return waiting;
            }
        }
    }

    internal sealed class InterruptibleBusyPeriodStrand<TState, TWork> : BusyPeriodStrand<TState, TWork>
    {
        private readonly Func<TState, CancellationToken, UniTask> _deactivate;

        internal InterruptibleBusyPeriodStrand(
            TState state,
            Func<TState, UniTask> activate,
            Func<TState, TWork, UniTask> invoke,
            Func<TState, CancellationToken, UniTask> deactivate) : base(state, activate, invoke) =>
            _deactivate = deactivate ?? throw new ArgumentNullException(nameof(deactivate));

        protected override InterruptibleDeactivateAttempt BeginDeactivate(Period period) => period.StartInterruptibleDeactivate();

        protected override async UniTask Deactivate(InterruptibleDeactivateAttempt interruption)
        {
            // WhenAny observes completion, then this method awaits the same callback to propagate its outcome.
            var callback = _deactivate(State, interruption.Cancellation).Preserve();
            if (!(await UniTask.WhenAny(interruption.Interruption, callback)).hasResultLeft)
            {
                await callback;
                return;
            }

            Exception cancellationFailure = null;
            try
            {
                interruption.Cancel();
            }
            catch (Exception failure)
            {
                cancellationFailure = failure;
            }

            try
            {
                await callback;
            }
            catch
            {
                if (cancellationFailure == null) throw;
            }

            if (cancellationFailure != null) throw cancellationFailure;
        }
    }

    internal sealed class UninterruptibleBusyPeriodStrand<TState, TWork> : BusyPeriodStrand<TState, TWork>
    {
        private readonly Func<TState, UniTask> _deactivate;

        internal UninterruptibleBusyPeriodStrand(
            TState state,
            Func<TState, UniTask> activate,
            Func<TState, TWork, UniTask> invoke,
            Func<TState, UniTask> deactivate) : base(state, activate, invoke) =>
            _deactivate = deactivate ?? throw new ArgumentNullException(nameof(deactivate));

        protected override InterruptibleDeactivateAttempt BeginDeactivate(Period period)
        {
            Close(period);
            return null;
        }

        protected override UniTask Deactivate(InterruptibleDeactivateAttempt interruption) => _deactivate(State);
    }
}
