export interface BackoffPolicy {
  baseDelayMs: number;
  maxDelayMs: number;
}

/**
 * Exponential backoff with equal jitter: between half and all of min(base * 2^(attempt - 1), max).
 * Jitter spreads retries of many failed rows so they do not hit a recovering broker in lockstep.
 */
export function backoffDelayMs(
  attempt: number,
  { baseDelayMs, maxDelayMs }: BackoffPolicy,
  random: () => number = Math.random,
): number {
  const exponential = Math.min(maxDelayMs, baseDelayMs * 2 ** Math.max(0, attempt - 1));
  return Math.floor(exponential / 2 + random() * (exponential / 2));
}
