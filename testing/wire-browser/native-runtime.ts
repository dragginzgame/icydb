// IcyDB owns native scalar semantics; IC Wire owns the shared codec and envelope.
import { createCodec as createWireCodec, WireError } from './wire-runtime.js';
export { WireError };

function decimal(value: unknown): void {
  if (typeof value !== 'string') throw new WireError('value');
  const text = value as string;
  if (text.length > 70 || !/^-?(0|[1-9][0-9]*)(\.[0-9]*[1-9])?$/.test(text) || text === '-0') throw new WireError('value');
  const parts = text.split('.');
  if (parts.length === 2 && parts[1].length > 28) throw new WireError('value');
  const mantissa = BigInt(parts.join(''));
  if (mantissa < -(1n << 127n) || mantissa >= (1n << 127n)) throw new WireError('value');
}

// Trusted metadata comes from concrete native Rust types, never remote input.
export function createCodec(metadata: unknown, registry: unknown) {
  return createWireCodec(metadata, registry, { decimal });
}
