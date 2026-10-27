// Type shim so the Next.js route can import the plain ESM worker.
declare module "*/scripts/business-ops.mjs" {
  export function artifactText(id: string, root?: string): string;
  export function enqueue(input: Record<string, unknown>, opts?: { root?: string }): unknown;
  export function readOperations(root?: string): unknown;
  export function retryJob(id: string, root?: string): unknown;
  export function setPaused(value: boolean, root?: string): boolean;
  export function saveMonitor(input: Record<string, unknown>, opts?: { root?: string }): unknown;
}
