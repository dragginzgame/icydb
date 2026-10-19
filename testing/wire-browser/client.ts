import type { HttpAgent } from '@icp-sdk/core/agent';

// Authentication, certificates, root keys and update polling stay with the
// caller's SDK agent. A generated contract owns bytes and the method identity.
export async function call<Q, R>(agent: HttpAgent, canisterId: string, contract: {
  methodName: string; mode: 'query' | 'update';
  encodeRequest(value: Q): Uint8Array; decodeResponse(bytes: Uint8Array): R;
}, request: Q): Promise<R> {
  const arg = contract.encodeRequest(request);
  if (contract.mode === 'update') {
    const result = await agent.update(canisterId, { methodName: contract.methodName, arg });
    return contract.decodeResponse(result.reply);
  }
  const result = await agent.query(canisterId, { methodName: contract.methodName, arg });
  if (result.status !== 'replied') throw new Error(`query rejected: ${result.reject_message}`);
  return contract.decodeResponse(new Uint8Array(result.reply.arg));
}
