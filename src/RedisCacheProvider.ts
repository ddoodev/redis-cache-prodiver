import { CacheProvider } from '@discordoo/providers'
import { AnyDiscordApplication, CompletedCacheOptions } from 'discordoo'
import { RedisClientType, createClient } from 'redis'

export enum RedisCacheProviderPerformanceMode {
  Fast,
  Saving
}

export type RedisCacheProviderOptions = Parameters<typeof createClient>[0] & {
  performance?: RedisCacheProviderPerformanceMode
}

export class RedisCacheProvider implements CacheProvider {
  public readonly redisClient: RedisClientType<any, any, any>

  public readonly app: AnyDiscordApplication
  public readonly compatible: 'classes' | 'json' | 'text' | 'buffer' = 'text'
  public readonly sharedCache = true
  public performanceMode: RedisCacheProviderPerformanceMode = RedisCacheProviderPerformanceMode.Fast

  constructor(app: AnyDiscordApplication, cacheOptions: CompletedCacheOptions, providerOptions: RedisCacheProviderOptions) {
    this.app = app
    this.redisClient = createClient(providerOptions)

    if (providerOptions.performance) {
      this.performanceMode = providerOptions.performance
    }
  }

  private getRedisKey(keyspace: string, storage?: string, key?: string) {
    return `${keyspace}:${storage ?? '*'}:${key ?? '*'}`
  }

  async get<K = string, V = any>(keyspace: string, storage: string, key: K): Promise<V | undefined> {
    return (await this.redisClient.get(
      this.getRedisKey(keyspace, storage, key as string)
    )) as V || undefined
  }
  async set<K = string, V = any>(keyspace: string, storage: string, key: K, value: V): Promise<CacheProvider> {
    await this.redisClient.set(
      this.getRedisKey(keyspace, storage, key as string),
      value as string
    )
    return this
  }
  async delete<K = string>(keyspace: string, storage: string, key: K | K[]): Promise<boolean> {
    const keys = (Array.isArray(key) ? key : [ key ]).map(key =>
      this.getRedisKey(keyspace, storage, key as string))
    return !!(await this.redisClient.del(keys))
  }
  async forEach<K = string, V = any, P extends CacheProvider = CacheProvider>(
    keyspace: string, storage: string, predicate: (value: V, key: K, provider: P) => unknown
  ): Promise<void> {
    const keys = await this.keys(keyspace, storage)

    if (!keys.length) return

    if (this.performanceMode === RedisCacheProviderPerformanceMode.Fast) {
      const values = await this.redisClient.mGet(
        keys.map(key => this.getRedisKey(keyspace, storage, key))
      )
  
      for (let i = 0; i < keys.length; i++) {
        if (!values[i]) continue
        await predicate(values[i] as V, keys[i] as K, this as unknown as P)
      }
    } else {
      for (const key of keys) {
        const value = await this.get(keyspace, storage, key)

        if (value) {
          await predicate(value as V, key as K, this as unknown as P)
        }
      }
    }
  }
  async clear(keyspace: string, storage: string): Promise<boolean> {
    const keys = await this.keys(keyspace, storage)

    if (!keys.length) return false

    return await this.delete(keyspace, storage, keys)
  }
  async size(keyspace: string, storage: string): Promise<number> {
    return (await this.keys(keyspace, storage)).length 
  }
  async has<K = string>(keyspace: string, storage: string, key: K): Promise<boolean> {
    return !!(await this.redisClient.exists(
      this.getRedisKey(keyspace, storage, key as string)
    ))
  }
  async sweep<K = string, V = any, P extends CacheProvider = CacheProvider>(
    keyspace: string, storage: string, predicate: (value: V, key: K, provider: P) => boolean | Promise<boolean>
  ): Promise<void> {
    const keys = await this.keys(keyspace, storage)

    if (!keys.length) return

    const keysToDelete: string[] = []

    if (this.performanceMode === RedisCacheProviderPerformanceMode.Fast) {
      const values = await this.redisClient.mGet(
        keys.map(key => this.getRedisKey(keyspace, storage, key))
      )
  
      const keysToDelete: string[] = []

      for (let i = 0; i < keys.length; i++) {
        if (!values[i]) continue
        if (await predicate(values[i] as V, keys[i] as K, this as unknown as P)) {
          keysToDelete.push(keys[i] as string)
        }
      }
    } else {
      for (const key of keys) {
        const value = await this.get(keyspace, storage, key)

        if (value && await predicate(value as V, key as K, this as unknown as P)) {
          keysToDelete.push(key as string)
        }
      }
    }

    if (keysToDelete.length) {
      await this.delete(keyspace, storage, keysToDelete)
    }
  }
  async filter<K = string, V = any, P extends CacheProvider = CacheProvider>(
    keyspace: string, storage: string, predicate: (value: V, key: K, provider: P) => boolean | Promise<boolean>
  ): Promise<[K, V][]> {
    const keys = await this.keys(keyspace, storage)

    if (!keys.length) return []

    const filtered: [K, V][] = []

    if (this.performanceMode === RedisCacheProviderPerformanceMode.Fast) {
      const values = await this.redisClient.mGet(
        keys.map(key => this.getRedisKey(keyspace, storage, key))
      )
  
      for (let i = 0; i < keys.length; i++) {
        if (!values[i]) continue

        if (await predicate(values[i] as V, keys[i] as K, this as unknown as P)) {
          filtered.push([ keys[i] as K, values[i] as V ])
        }
      }
    } else {
      for (const key of keys) {
        const value = await this.get(keyspace, storage, key)

        if (value && await predicate(value as V, key as K, this as unknown as P)) {
          filtered.push([ key as K, value as V ])
        }
      }
    }

    return filtered
  }
  async map<K = string, V = any, R = any, P extends CacheProvider = CacheProvider>(
    keyspace: string, storage: string, predicate: (value: V, key: K, provider: P) => R | Promise<R>
  ): Promise<R[]> {
    const keys = await this.keys(keyspace, storage)

    if (!keys.length) return []

    const mapped: R[] = []

    if (this.performanceMode === RedisCacheProviderPerformanceMode.Fast) {
      const values = await this.redisClient.mGet(
        keys.map(key => this.getRedisKey(keyspace, storage, key))
      )

      for (let i = 0; i < keys.length; i++) {
        if (!values[i]) continue

        mapped.push(await predicate(values[i] as V, keys[i] as K, this as unknown as P))
      }
    } else {
      for (const key of keys) {
        const value = await this.get(keyspace, storage, key)

        if (value) {
          mapped.push(await predicate(value as V, key as K, this as unknown as P))
        }
      }
    }

    return mapped
  }
  async find<K = string, V = any, P extends CacheProvider = CacheProvider>(
    keyspace: string, storage: string, predicate: (value: V, key: K, provider: P) => boolean | Promise<boolean>
  ): Promise<V | undefined> {
    const keys = await this.keys(keyspace, storage)

    if (!keys.length) return undefined

    if (this.performanceMode === RedisCacheProviderPerformanceMode.Fast) {
      const values = await this.redisClient.mGet(
        keys.map(key => this.getRedisKey(keyspace, storage, key))
      )

      for (let i = 0; i < keys.length; i++) {
        if (!values[i]) continue

        if (await predicate(values[i] as V, keys[i] as K, this as unknown as P)) {
          return values[i] as V
        }
      }
    } else {
      for (const key of keys) {
        const value = await this.get(keyspace, storage, key)

        if (value && await predicate(value as V, key as K, this as unknown as P)) {
          return value as V
        }
      }
    }

    return undefined
  }
  async count<K = string, V = any, P extends CacheProvider = CacheProvider>(
    keyspace: string, storage: string, predicate: (value: V, key: K, provider: P) => boolean | Promise<boolean>
  ): Promise<number> {
    const keys = await this.keys(keyspace, storage)

    if (!keys.length) return 0

    let count = 0

    if (this.performanceMode === RedisCacheProviderPerformanceMode.Fast) {
      const values = await this.redisClient.mGet(
        keys.map(key => this.getRedisKey(keyspace, storage, key))
      )

      for (let i = 0; i < keys.length; i++) {
        if (!values[i]) continue

        if (await predicate(values[i] as V, keys[i] as K, this as unknown as P)) {
          count++
        }
      }
    } else {
      for (const key of keys) {
        const value = await this.get(keyspace, storage, key)

        if (value && await predicate(value as V, key as K, this as unknown as P)) {
          count++
        }
      }
    }

    return count
  }
  async counts<K = string, V = any, P extends CacheProvider = CacheProvider>(
    keyspace: string, storage: string, predicates: ((value: V, key: K, provider: P) => boolean | Promise<boolean>)[]
  ): Promise<number[]> {
    const keys = await this.keys(keyspace, storage)

    if (!keys.length) return []

    const counts: number[] = []

    for (const predicate of predicates) {
      let count = 0

      if (this.performanceMode === RedisCacheProviderPerformanceMode.Fast) {
        const values = await this.redisClient.mGet(
          keys.map(key => this.getRedisKey(keyspace, storage, key))
        )

        for (let i = 0; i < keys.length; i++) {
          if (!values[i]) continue

          if (await predicate(values[i] as V, keys[i] as K, this as unknown as P)) {
            count++
          }
        }
      } else {
        for (const key of keys) {
          const value = await this.get(keyspace, storage, key)

          if (value && await predicate(value as V, key as K, this as unknown as P)) {
            count++
          }
        }
      }

      counts.push(count)
    }

    return counts
  }
  async keys<K = string>(keyspace: string, storage: string): Promise<K[]> {
    return (await this.redisClient.keys(
      this.getRedisKey(keyspace, storage, '*')
    ))
      .map(key => key.split(':')[2]) as K[]
  }
  async values<V = any>(keyspace: string, storage: string): Promise<V[]> {
    const keys = await this.keys(keyspace, storage)
    return (await this.redisClient.mGet(keys)) as V[]
  }
  async entries<K = string, V = any>(keyspace: string, storage: string): Promise<[K, V][]> {
    const keys = await this.keys(keyspace, storage)
    const values = (await this.redisClient.mGet(keys)) as V[]

    return keys.map((key, index) => [ key, values[index] ] as [K, V])
  }
  async init() {
    await this.redisClient.connect()
  }
}
