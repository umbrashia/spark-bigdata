import type { JvmHandle, JvmProxy } from './proxy';

export interface Node4jClient {
  newObject(className: string, ...args: unknown[]): Promise<unknown>;
  getStaticMember(className: string, memberName: string): Promise<unknown>;
  call(handle: unknown, methodName: string, ...args: unknown[]): Promise<unknown>;
}

export class Node4jGateway {
  private readonly client: Node4jClient;

  constructor(node4jClient: Node4jClient) {
    if (!node4jClient) {
      throw new Error('node4j client is required');
    }

    this.client = node4jClient;
  }

  async create(className: string, args: unknown[] = []): Promise<JvmHandle> {
    const handle = await this.client.newObject(className, ...args);
    return normalizeHandle(handle);
  }

  async getStatic(className: string, memberName: string): Promise<JvmHandle> {
    const handle = await this.client.getStaticMember(className, memberName);
    return normalizeHandle(handle);
  }

  async call(handle: JvmHandle | JvmProxy, methodName: string, args: unknown[] = []): Promise<unknown> {
    const rawHandle = unwrapHandle(handle);
    const result = await this.client.call(rawHandle, methodName, ...args.map(unwrapHandle));
    return normalizeResult(result);
  }
}

export function unwrapHandle(value: unknown): unknown {
  if (value && typeof value === 'object') {
    const maybeProxy = value as Partial<JvmProxy>;
    if (maybeProxy.__isJvmProxy && maybeProxy.__handle) {
      return maybeProxy.__handle.raw;
    }

    const maybeHandle = value as Partial<JvmHandle>;
    if (maybeHandle.__isJvmHandle && 'raw' in maybeHandle) {
      return maybeHandle.raw;
    }
  }

  return value;
}

export function normalizeHandle(raw: unknown): JvmHandle {
  return {
    __isJvmHandle: true,
    raw,
  };
}

export function normalizeResult(raw: unknown): unknown {
  if (raw && typeof raw === 'object' && '__javaObjectId' in (raw as Record<string, unknown>)) {
    return normalizeHandle(raw);
  }

  if (Array.isArray(raw)) {
    return raw.map((item) => normalizeResult(item));
  }

  return raw;
}
