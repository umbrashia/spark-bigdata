'use strict';

class Node4jGateway {
  constructor(node4jClient) {
    if (!node4jClient) {
      throw new Error('node4j client is required');
    }

    this.client = node4jClient;
  }

  async create(className, args = []) {
    const handle = await this.client.newObject(className, ...args);
    return normalizeHandle(handle);
  }

  async getStatic(className, memberName) {
    const handle = await this.client.getStaticMember(className, memberName);
    return normalizeHandle(handle);
  }

  async call(handle, methodName, args = []) {
    const rawHandle = unwrapHandle(handle);
    const result = await this.client.call(rawHandle, methodName, ...args.map(unwrapHandle));
    return normalizeResult(result);
  }
}

function unwrapHandle(value) {
  if (value && value.__isJvmProxy && value.__handle) {
    return value.__handle.raw;
  }

  if (value && value.__isJvmHandle) {
    return value.raw;
  }

  return value;
}

function normalizeHandle(raw) {
  return {
    __isJvmHandle: true,
    raw,
  };
}

function normalizeResult(raw) {
  if (raw && typeof raw === 'object' && raw.__javaObjectId) {
    return normalizeHandle(raw);
  }

  if (Array.isArray(raw)) {
    return raw.map((item) => normalizeResult(item));
  }

  return raw;
}

module.exports = {
  Node4jGateway,
  normalizeHandle,
  normalizeResult,
  unwrapHandle,
};
