import { CacheProvider } from '@discordoo/providers'
import { AnyDiscordApplication, CompletedCacheOptions } from 'discordoo'
import { Cluster, ClusterNode, ClusterOptions, Redis, RedisOptions } from 'ioredis'

export enum RedisCacheProviderConnectionType {
  Standalone,
  Cluster
}

type RedisClusterOptions = ClusterOptions & {
  nodes: ClusterNode[]
}

export type RedisCacheProviderOptions = (RedisOptions | RedisClusterOptions) & {
  connection?: RedisCacheProviderConnectionType,
  prefix?: string
}

export class RedisCacheProvider implements CacheProvider {
  public readonly redisClient: Redis | Cluster

  public readonly app: AnyDiscordApplication
  public readonly compatible: 'classes' | 'json' | 'text' | 'buffer' = 'text'
  public readonly sharedCache = true

  public readonly connectionType: RedisCacheProviderConnectionType = RedisCacheProviderConnectionType.Standalone
  public readonly prefix?: string

  constructor(app: AnyDiscordApplication, cacheOptions: CompletedCacheOptions, providerOptions: RedisCacheProviderOptions) {
    this.connectionType = providerOptions.connection ?? RedisCacheProviderConnectionType.Standalone

    if (providerOptions.prefix) {
      this.prefix = providerOptions.prefix
    }

    this.app = app
    this.redisClient = this.connectionType === RedisCacheProviderConnectionType.Standalone ?
      new Redis({ ...providerOptions, lazyConnect: true }) :
        new Cluster((providerOptions as RedisClusterOptions).nodes, { ...providerOptions, lazyConnect: true })
  }

  private getStorageKey(keyspace: string, storage: string) {
    return `${this.prefix ? (this.prefix + ':') : ''}${keyspace}:${storage}`
  }

  async get<K = string, V = any>(keyspace: string, storage: string, key: K): Promise<V | undefined> {
    return (await this.redisClient.hget(
      this.getStorageKey(keyspace, storage),
      key as string
    )) as V || undefined
  }
  async set<K = string, V = any>(keyspace: string, storage: string, key: K, value: V): Promise<CacheProvider> {
    await this.redisClient.hset(
      this.getStorageKey(keyspace, storage),
      key as string,
      value as string
    )
    return this
  }
  async delete<K = string>(keyspace: string, storage: string, key: K | K[]): Promise<boolean> {
    const fields = Array.isArray(key) ? key : [ key ]
    return !!(await this.redisClient.hdel(
      this.getStorageKey(keyspace, storage),
      ...fields as string[]
    ))
  }
  async forEach<K = string, V = any, P extends CacheProvider = CacheProvider>(
    keyspace: string, storage: string, predicate: (value: V, key: K, provider: P) => unknown
  ): Promise<void> {
    for (const [ key, value ] of await this.entries(keyspace, storage)) {
      await predicate(value as V, key as K, this as unknown as P)
    }
  }
  async clear(keyspace: string, storage: string): Promise<boolean> {
    return !!(await this.redisClient.del(
      this.getStorageKey(keyspace, storage)
    ))
  }
  async size(keyspace: string, storage: string): Promise<number> {
    return await this.redisClient.hlen(
      this.getStorageKey(keyspace, storage)
    ) 
  }
  async has<K = string>(keyspace: string, storage: string, key: K): Promise<boolean> {
    return !!(await this.redisClient.hexists(
      this.getStorageKey(keyspace, storage),
      key as string
    ))
  }
  async keys<K = string>(keyspace: string, storage: string): Promise<K[]> {
    return (await this.redisClient.hkeys(
      this.getStorageKey(keyspace, storage)
    )) as K[]
  }
  async values<V = any>(keyspace: string, storage: string): Promise<V[]> {
    return (await this.redisClient.hvals(
      this.getStorageKey(keyspace, storage)
    )) as V[]
  }
  async entries<K = string, V = any>(keyspace: string, storage: string): Promise<[K, V][]> {
    return Object.entries(await this.redisClient.hgetall(
      this.getStorageKey(keyspace, storage)
    )) as [K, V][]
  }
  async init() {
    await this.redisClient.connect()
  }
}
