export type MethodTransformer = (name: string) => string;

export interface JvmHandle {
  __isJvmHandle: true;
  raw: unknown;
}

export interface JvmProxy {
  __handle: JvmHandle;
  __isJvmProxy: true;
  [key: string]: (...args: unknown[]) => unknown;
}

export interface GatewayLike {
  call(handle: JvmHandle, methodName: string, args?: unknown[]): unknown;
}

interface ProxyOptions {
  methodTransformer?: MethodTransformer;
}

const DEFAULT_OPTIONS: Required<ProxyOptions> = {
  methodTransformer: (name) => name,
};

function isPromise<T>(value: T | Promise<T>): value is Promise<T> {
  return !!value && typeof (value as Promise<T>).then === 'function';
}

export function createJvmProxy(gateway: GatewayLike, handle: JvmHandle, options: ProxyOptions = {}): JvmProxy {
  const resolvedOptions = { ...DEFAULT_OPTIONS, ...options };

  const resolveWrap = (value: unknown): unknown => {
    if (value && typeof value === 'object' && '__isJvmHandle' in (value as Record<string, unknown>)) {
      return createJvmProxy(gateway, value as JvmHandle, resolvedOptions);
    }

    if (Array.isArray(value)) {
      return value.map((item) => resolveWrap(item));
    }

    return value;
  };

  const proxyTarget = function proxyTarget() {};

  return new Proxy(proxyTarget, {
    get(_, property) {
      if (property === '__handle') {
        return handle;
      }

      if (property === '__isJvmProxy') {
        return true;
      }

      if (property === 'then') {
        return undefined;
      }

      if (typeof property === 'symbol') {
        return undefined;
      }

      return (...args: unknown[]) => {
        const methodName = resolvedOptions.methodTransformer(property);
        const result = gateway.call(handle, methodName, args);

        if (isPromise(result)) {
          return result.then((value) => resolveWrap(value));
        }

        return resolveWrap(result);
      };
    },
  }) as unknown as JvmProxy;
}
