/** Injectable time source so tests can pin timestamps and backoff deadlines. */
export type Clock = () => Date;

export const systemClock: Clock = () => new Date();

export function isoNow(clock: Clock): string {
  return clock().toISOString();
}

export function isoAfter(clock: Clock, delayMs: number): string {
  return new Date(clock().getTime() + delayMs).toISOString();
}
