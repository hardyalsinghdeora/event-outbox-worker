import { setTimeout as sleep } from 'node:timers/promises';

export interface PollLoop {
  start(): void;
  /** Resolves once the tick in progress has finished; nothing is interrupted mid-batch. */
  stop(): Promise<void>;
}

export interface PollLoopOptions {
  /** One unit of work. Resolves `true` when there was work, so the loop polls again without sleeping. */
  tick: () => Promise<boolean>;
  idleDelayMs: number;
  onError: (error: unknown) => void;
}

/** Shared loop for the relay and the queue broker: busy-poll while there is work, sleep when idle, stop between ticks. */
export function createPollLoop({ tick, idleDelayMs, onError }: PollLoopOptions): PollLoop {
  let running: Promise<void> | undefined;
  let controller: AbortController | undefined;

  const run = async (signal: AbortSignal): Promise<void> => {
    while (!signal.aborted) {
      let busy = false;
      try {
        busy = await tick();
      } catch (error) {
        // A failing tick falls through to the idle sleep, so persistent errors cannot hot-loop.
        onError(error);
      }
      if (busy) continue;
      try {
        await sleep(idleDelayMs, undefined, { signal });
      } catch {
        // Woken early by stop(); the while condition ends the loop.
      }
    }
  };

  return {
    start() {
      if (running) return;
      controller = new AbortController();
      running = run(controller.signal);
    },
    async stop() {
      controller?.abort();
      await running;
      running = undefined;
      controller = undefined;
    },
  };
}
