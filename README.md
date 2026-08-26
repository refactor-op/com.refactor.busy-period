# Refactor.BusyPeriod 0.1.0

Serializes work into busy periods with an explicit activation lifecycle.

```csharp
var strand = BusyPeriod.Interruptible<State, Work>(
    state,
    activate: ActivateAsync,
    invoke: InvokeAsync,
    deactivate: DeactivateAsync);

BusyPeriodResponse response = strand.Dispatch(work);
await response.Work;
await response.Period;
```

## Installation

Add this repository through Unity Package Manager using its Git URL, then add UniTask to the project manifest because
Unity resolves Git dependencies only from `Packages/manifest.json`:

```json
{
  "dependencies": {
    "com.refactor.busy-period": "https://github.com/refactor-op/com.refactor.busy-period.git",
    "com.cysharp.unitask": "https://github.com/Cysharp/UniTask.git?path=src/UniTask/Assets/Plugins/UniTask#2e993ff18f28c931602a07292df0b0804eebef99"
  }
}
```

`activate` establishes the state invariant required by `invoke`. It may run again after an interrupted `deactivate`.
`deactivate` releases that active invariant. In the interruptible form, work arriving during `deactivate` joins the
current period, cancels the deactivation, waits for it to settle, and activates the state again before invoking work.

Use `Uninterruptible` when work arriving during `deactivate` must wait for a new period instead.

`response.Work` completes for one submitted work item and has a single consumer. `response.Period` completes for the busy
period that admitted it and can be awaited by multiple consumers. Dispatch is thread-safe; callbacks may complete synchronously before
`Dispatch` returns.

User callback failures are reported through the corresponding completion task: activation and deactivation failures fail
the period, while invocation failures affect only that work item. A cancellation caused by joining work is control flow.

The package targets Unity 6000.3 and depends on UniTask 2.5.11.
