/** One-line description of an error for logs and dead-letter rows. */
export function describeError(error: unknown): string {
  return error instanceof Error ? `${error.name}: ${error.message}` : String(error);
}
