'use strict';

const DEFAULT_OPTIONS = {
  methodTransformer: (name) => name,
};

function isPromise(value) {
  return value && typeof value.then === 'function';
}

function createJvmProxy(gateway, handle, options = {}) {
  const resolvedOptions = { ...DEFAULT_OPTIONS, ...options };

  const resolveWrap = (value) => {
    if (value && value.__isJvmHandle) {
      return createJvmProxy(gateway, value, resolvedOptions);
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

      return (...args) => {
        const methodName = resolvedOptions.methodTransformer(property);
        const result = gateway.call(handle, methodName, args);

        if (isPromise(result)) {
          return result.then((value) => resolveWrap(value));
        }

        return resolveWrap(result);
      };
    },
  });
}

module.exports = {
  createJvmProxy,
};
