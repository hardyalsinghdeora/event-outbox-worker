import { ZodError } from 'zod';

/** One-line description of an error for logs and dead-letter rows. */
export function describeError(error: unknown): string {
  if (error instanceof ZodError) {
    const issues = error.issues.map((issue) => `${issue.path.map(String).join('.')}: ${issue.message}`);
    return `ZodError: ${issues.join('; ')}`;
  }
  if (error instanceof Error) return `${error.name}: ${error.message.replace(/\s+/g, ' ').trim()}`;
  return String(error);
}
