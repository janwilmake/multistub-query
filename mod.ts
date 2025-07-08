//@ts-check
/// <reference types="@cloudflare/workers-types" />
import { RemoteSqlStorageCursor, SqlStorageRow, exec } from "remote-sql-cursor";
import { getStubs, MultiStubConfig } from "multistub";

/**
 * Execute SQL query in multiple DOs at the same time returning only the first cursor
 */
export function multistubQuery<T extends SqlStorageRow>(
  doNamespace: DurableObjectNamespace<any>,
  ctx: ExecutionContext,
  configs: MultiStubConfig[],
  sql: string,
  ...params: any[]
): RemoteSqlStorageCursor<T> {
  const stubs = getStubs(doNamespace, configs); // Main stub is the first one
  const [mainStub, ...mirrorStubs] = stubs;
  const cursor = exec<T>(mainStub, sql, ...params);

  // Execute on mirrors if configured and initialized
  if (mirrorStubs.length > 0 && ctx) {
    const mirrorPromise = async () => {
      try {
        // Execute the same query on all mirrors
        await Promise.all(
          mirrorStubs.map(async (mirrorStub) => {
            try {
              for await (const _ of exec(mirrorStub, sql, ...params)) {
                // Do nothing, just ensure it's processed
              }
            } catch (error) {
              console.error("Mirror execution error:", error);
            }
          }),
        );
      } catch (error) {
        console.error("Mirror execution error:", error);
      }
    };

    // Use waitUntil if context provided, otherwise fire and forget
    ctx.waitUntil(mirrorPromise());
  }

  return cursor;
}
