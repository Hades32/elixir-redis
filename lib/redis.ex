defmodule Redis do
  # defined basic types
  @type key :: binary | atom
  @type channel :: binary | atom
  @type source :: binary | atom
  @type message :: binary | atom
  @type destination :: binary | atom
  @type newkey :: binary | atom
  @type value :: binary | atom | integer
  @type timestamp :: integer
  @type index :: integer
  @type max :: integer
  @type start :: integer
  @type stop :: integer
  @type min :: integer
  @type count :: integer
  @type milliseconds :: integer
  @type decrement :: integer
  @type increment :: integer
  @type limit :: [offset: integer, limit: integer]
  @type ttl :: integer
  @type cursor :: integer
  @type offset :: integer
  @type a :: integer
  @type b :: integer
  @type field :: binary | atom | integer
  @type sts_reply :: :ok | binary
  @type int_reply :: integer
  @type bool_reply :: boolean
  @type hash_reply :: binary
  @type as_is :: binary
  @type ok_reply :: tuple
  @type name :: atom | []
  @type keys :: atom | []
  @type secs :: integer

  def start() do
    :gen_server.start( {:local, :redis}, Redis.Server, [], [])
  end

  def start_link() do
    :gen_server.start_link( {:local, :redis}, Redis.Server, [], [])
  end

  @spec connect([Keyword.t] | []) :: {:ok, pid} | {:error, Reason::term()}
  def connect(options\\[]) do
    :gen_server.start_link(Redis.Server, options, [])
  end

  def stop(pid) do
    :gen_server.cast(pid || client, {:stop})
  end

  funs = [
      #Keys
      # DEL key [key ...] - Delete a key
      {:del, [:key], :int_reply},

      # DUMP DUMP key - Return a serialized version of
      #  the value stored at the specified key
      {:dump, [:key], :sts_reply},

      # EXISTS key - Determine if a key exists
      {:exists, [:key], :bool_reply},

      # EXPIRE key seconds - Set a key's time to live in seconds
      {:expire, [:key, :value], :bool_reply},
      # EXPIREAT key timestamp - Set the expiration 
      # for a key as a UNIX timestamp
      {:expireat, [:key, :timestamp], :bool_reply},

      # KEYS pattern - Find all keys matching the given pattern
      {:keys, [:key], :sts_reply},

      # OBJECT subcommand [arguments [arguments ...]]
      # Inspect the internals of Redis objects
      {:object, [:key, :value], :sts_reply},

      # PERSIST key - Remove the expiration from a key
      {:persist, [:key], :bool_reply},

      # PEXPIRE key milliseconds - Set a key's time to live in milliseconds
      {:pexpire, [:key, :milliseconds], :bool_reply},

      # PEXPIREAT key milliseconds-timestamp
      # Set the expiration for a key as a UNIX timestamp specified in milliseconds
      {:pexpireat, [:key, :timestamp], :bool_reply},

      # PTTL key - Get the time to live for a key in milliseconds
      {:pttl, [:milliseconds], :int_reply},

      # RANDOMKEY - Return a random key from the keyspace
      {:randomkey, [], :sts_reply},

      # RENAME key newkey - Rename a key
      {:rename, [:key, :newkey], :ok_reply},

      # RENAMENX key newkey - Rename a key, only if the new key does not exist
      {:renamenx, [:key, :newkey], :int_reply},

      # RESTORE key ttl serialized-value
      # Create a key using the provided serialized value,
      # previously obtained using DUMP.
      {:restore, [:key, :ttl, :value], :sts_reply},

      # TTL key - Get the time to live for a key
      {:ttl, [:key], :int_reply},

      # TYPE key - Determine the type stored at key
      {:type, [:key], :sts_reply},

      # APPEND key value - Append a value to a key
      {:append, [:key, :value], :int_reply},

      # BITCOUNT key start end [start end ...]
      # Count set bits in a string
      {:bitcount, [:key, :a, :b], :int_reply},

      # DECR key - Decrement the integer value of a key by one
      {:decr, [:key], :int_reply},

      # DECRBY key decrement
      # Decrement the integer value of a key by the given number
      {:decrby, [:key, :decrement], :int_reply},

      # GET key - Get the value of a key
      {:get, [:key], :as_is},

      # GETBIT key offset
      # Returns the bit value at offset in the string value stored at key
      {:getbit, [:key, :offset], :int_reply},

      # GETRANGE key start end
      # Get a substring of the string stored at a key
      {:getrange, [:key, :a, :b], :as_is},

      # GETSET key value
      # Set the string value of a key and return its old value
      {:getset, [:key, :value], :sts_reply},

      # INCR key - Increment the integer value of a key by one
      {:incr, [:key], :int_reply},

      # INCRBY key increment
      # Increment the integer value of a key by the given amount
      {:incrby, [:key, :increment], :int_reply},

      # PSETEX key milliseconds value
      # Set the value and expiration in milliseconds of a key
      {:psetex, [:key, :milliseconds, :value], :sts_reply},

      # SETBIT key offset value
      # Sets or clears the bit at offset in the string value stored at key
      {:setbit, [:key, :offset, :value], :int_reply},

      # SETNX key value
      # Set the value of a key, only if the key does not exist
      {:setnx, [:key, :value], :int_reply},

      # SETRANGE key offset value
      # Overwrite part of a string at key starting at the specified offset
      {:setrange, [:key, :offset, :value], :int_reply},

      # STRLEN key - Get the length of the value stored in a key
      {:strlen, [:key], :int_reply},

      # HEXISTS key field - Determine if a hash field exists
      {:hexists, [:key, :field], :bool_reply},

      # HGET key field - Get the value of a hash field
      {:hget, [:key, :field], :sts_reply},

      # HGETALL key
      # Get all the fields and values in a hash
      {:hgetall, [:key], :hash_reply},

      # HINCRBY key field increment
      # Increment the integer value of a hash field by the given number
      {:hincrby, [:key, :field, :increment], :int_reply},

      # HKEYS key - Get all the fields in a hash
      {:hkeys, [:key], :hash_reply},

      # HLEN key - Get the number of fields in a hash
      {:hlen, [:key], :int_reply},

      # HSET key field value - Set the string value of a hash field
      {:hset, [:key, :field, :value], :int_reply},

      # HSETNX key field value
      # Set the value of a hash field, only if the field does not exist
      {:hsetnx, [:key, :field, :value], :int_reply},

      # HVALS key - Get all the values in a hash
      {:hvals, [:key], :hash_reply},

      # LINDEX key index - Get an element from a list by its index
      {:lindex, [:key, :index], :sts_reply},

      # LLEN key - Get the length of a list
      {:llen, [:key], :int_reply},

      # LPOP key - Remove and get the first element in a list
      {:lpop, [:key], :sts_reply},

      # LPUSHX key value - Prepend a value to a list, only if the list exists
      {:lpushx, [:key, :value], :int_reply},

      # LRANGE key start stop - Get a range of elements from a list
      {:lrange, [:key, :a, :b], :sts_reply},

      # LREM key count value - Remove elements from a list
      {:lrem, [:key, :count, :value], :int_reply},

      # LSET key index value
      # Set the value of an element in a list by its index
      {:lset, [:key, :index, :value], :sts_reply},

      # LTRIM key start stop - Trim a list to the specified range
      {:ltrim, [:key, :a, :b], :sts_reply},

      # RPOP key - Remove and get the last element in a list
      {:rpop, [:key], :sts_reply},

      # RPOPLPUSH source destination
      # Remove the last element in a list, append it to another list and return it
      {:rpoplpush, [:source, :destination], :sts_reply},

      # RPUSHX key value - Append a value to a list, only if the list exists
      {:rpushx, [:key, :value], :int_reply},


      # SCARD key - Get the number of members in a set
      {:scard, [:key], :int_reply},


      # SISMEMBER key member - Determine if a given value is a member of a set
      {:sismember, [:key, :field], :int_reply},

      # SMEMBERS key - Get all the members in a set
      {:smembers, [:key], :sts_reply},

      # SMOVE source destination member
      # Move a member from one set to another
      {:smove, [:source, :destination, :field], :bool_reply},

      # SPOP key - Remove and return a random member from a set
      {:spop, [:key], :sts_reply},

      # ZCARD key -  Get the number of members in a sorted set
      {:zcard, [:key], :int_reply},

      # ZCOUNT key min max
      # Count the members in a sorted set with scores within the given values
      {:zcount, [:key, :min, :max], :int_reply},

      # ZINCRBY key increment member
      # Increment the score of a member in a sorted set
      {:zincrby, [:key, :increment, :field], :int_reply},

      # ZLEXCOUNT key min max
      # Count the number of members in a sorted set between a given lexicographical range
      {:zlexcount, [:key, :min, :max], :int_reply},

      # ZRANK key member - Determine the index of a member in a sorted set
      {:zrank, [:key, :field], :int_reply},


      # ZREMRANGEBYLEX key min max
      # Remove all members in a sorted set between the given lexicographical range
      {:zremrangebylex, [:key, :min, :max], :int_reply},

      # ZREMRANGEBYRANK key start stop
      # Remove all members in a sorted set within the given indexes
      {:zremrangebyrank, [:key, :start, :stop], :int_reply},

      # ZREMRANGEBYSCORE key min max
      # Remove all members in a sorted set within the given scores
      {:zremrangebyscore, [:key, :min, :max], :int_reply},


      # ZREVRANK key member
      # Determine the index of a member in a sorted set, with scores ordered from high to low
      {:zrevrank, [:key, :field], :int_reply},

      # ZSCORE key member
      # Get the score associated with the given member in a sorted set
      {:zscore, [:key, :field], :sts_reply},


      # PUBLISH channel message - Post a message to a channel
      {:publish, [:channel, :message], :int_reply},
  ]


  # SORT key [BY pattern] [LIMIT offset count]
  # [GET pattern [GET pattern ...]] [ASC|DESC]
  # [ALPHA] [STORE destination]
  # Sort the elements in a list, set or sorted set
  @spec sort(pid, key) :: sts_reply
  def sort(pid \\ nil, key) do
    sort_opt(pid, key, [])
  end

  @spec sort_opt(pid, key, any) :: sts_reply
  def sort_opt(pid \\ nil, key, opts) do
    params = [key]
    by = opts[:by]   # by: bla_*
    limit = opts[:limit]  # limit: [offset: 0, count: 10]
    get = opts[:get]  # get: ["key_*", "bla_"] or "key_*"
    desc = opts[:get]  # desc: true
    alpha = opts[:alpha]  # alpha: true
    store = opts[:store]  # strore: :newkey_for_result
    if by do
      params = params ++ ["BY", by]
    end
    if limit do
      params = params ++ ["LIMIT", limit[:offset], limit[:count]]
    end
    if get do
      params = params ++ cond do
        is_list(get) ->
          Enum.reduce(Enum.map(get, &["GET", &1]), &(&1 ++ &2))
        get ->
          ["GET", get]
      end
    end
    if desc == true do 
      params = params ++ ["DESC"]
    end
    if alpha == true do 
      params = params ++ ["ALPHA"]
    end
    if store do
      params = params ++ ["STORE", store]
    end
    call_server(pid, {:raw, :sort, params}) |> sts_reply
  end

  # BITOP operation destkey key [key ...]
  # Perform bitwise operations between strings
  @spec bitop(pid, binary, key, []) :: sts_reply
  def bitop(pid \\ nil, operation, destkey, keys)
     when operation in [:and, :or, :xor, :not] do
    params = [Atom.to_string(operation) |> String.upcase, destkey] ++ keys
    call_server(pid, {:raw, :bitop, params}) |> int_reply
  end


  # SCAN cursor [MATCH pattern] [COUNT count]
  # Incrementally iterate the keys space
  @spec scan(pid, key) :: sts_reply
  def scan(pid \\ nil, key), do: scan_opt(pid, key, [])

  @spec scan_opt(pid, key, any) :: sts_reply
  def scan_opt(pid \\ nil, key, opts) do
    params = [key]
    if opts[:match] do
      params = params ++ ["MATCH", opts[:match]]
    end

    if opts[:count] do
      params = params ++ ["COUNT", opts[:count]]
    end
    call_server(pid, {:raw, :scan, params}) |> sts_reply
  end


  # BITPOS key bit [start] [end]
  # Find first bit set or clear in a string
  @spec bitpos(pid, key, integer) :: int_reply
  def bitpos(pid \\ nil, key, bit), do: bitpos_opt(pid, key, bit, [])

  @spec bitpos_opt(pid, key, integer, any) :: int_reply
  def bitpos_opt(pid \\ nil, key, bit, opts) do
    params = [key, bit]
    if opts[:start] do
      params = params ++ [opts[:start]]
    end
    if opts[:end] do
      params = params ++ [opts[:end]]
    end
    call_server(pid, {:raw, :bitpos, params}) |> int_reply
  end

  # SETEX key seconds value
  # Set the value and expiration of a key
  @spec setex(pid, key, secs, value) :: sts_reply
  def setex(pid \\ nil, key, secs, value) do
    call_server(pid, {:setex, key, secs, value}) |> sts_reply
  end

  # MGET key [key ...]
  # Get the values of all the given keys
  @spec mget(pid, []) :: sts_reply
  def mget(pid \\ nil, keys) do
    if not is_list(keys) do
      keys = [keys]
    end
    call_server(pid, List.to_tuple([:mget] ++ keys)) |> sts_reply
  end

  # MSET key value [key value ...]
  # Set multiple keys to multiple values
  @spec mset(pid, []) :: sts_reply
  def mset(pid \\ nil, kvs) do
    kvs = kvs |> Enum.flat_map(fn {key, val} -> [key, val] end)
    call_server(pid, {:raw, :mset, kvs}) |> sts_reply
  end

  # MSETNX key value [key value ...]
  # Set multiple keys to multiple values, only if none of the keys exist
  @spec msetnx(pid, []) :: sts_reply
  def msetnx(pid \\ nil, kvs) do
    kvs = kvs |> Enum.flat_map(fn {key, val} -> [key, val] end)
    call_server(pid, {:raw, :msetnx, kvs}) |> sts_reply
  end

  # SET key value [EX seconds] [PX milliseconds] [NX|XX]
  # Set the string value of a key
  @spec set(pid, key, value) :: sts_reply
  def set(pid \\ nil, key, value) when (is_pid(pid) or pid == nil), do: set_opt(pid, key, value, [])

  @spec set_opt(pid, key, value, any) :: sts_reply
  def set_opt(pid \\ nil, key, value, opts) when (is_pid(pid) or pid == nil) and is_list(opts) do
    params = [key, value]
    ex = opts[:ex]   # Set the specified expire time, in seconds
    px = opts[:px]   # Set the specified expire time, in milliseconds.
    nx = opts[:nx]   # Only set the key if it does not already exist.
    xx = opts[:xx]   # Only set the key if it already exist.
    if ex do
      params = params ++ ["EX", ex]
    end
    if px do
      params = params ++ ["PX", px]
    end
    if nx do
      params = params ++ ["NX"]
    else
      if xx do
        params = params ++ ["XX"]
      end
    end
    call_server(pid, {:raw, :set, params}) |> sts_reply
  end

  # HDEL key field [field ...] - Delete one or more hash fields
  @spec hdel(pid, key, []) :: int_reply
  def hdel(pid \\ nil, key, fields) do
    call_server(pid, {:raw, :hdel, [key] ++ fields}) |> int_reply
  end

  # HMGET key field [field ...]
  # Get the values of all the given hash fields
  @spec hmget(pid, key, []) :: hash_reply
  def hmget(pid \\ nil, key, fields) do
    call_server(pid, {:raw, :hmget, [key] ++ fields}) |> hash_reply
  end

  # HMSET key field value [field value ...]
  # Set multiple hash fields to multiple values
  @spec hmset(pid, key, []) :: sts_reply
  def hmset(pid \\ nil, key, kvs) do
    kvs = kvs |> Enum.flat_map(fn {key, val} -> [key, val] end)
    call_server(pid, {:raw, :hmset, [key] ++ kvs}) |> sts_reply
  end

  # HSCAN key cursor [MATCH pattern] [COUNT count]
  # Incrementally iterate hash fields and associated values
  @spec hscan(pid, key, cursor) :: hash_reply
  def hscan(pid \\ nil, key, cursor), do: hscan_opt(pid, key, cursor, [])

  @spec hscan_opt(pid, key, cursor, any) :: hash_reply
  def hscan_opt(pid \\ nil, key, cursor, opts) do
    params = [key, cursor]
    if opts[:match] do
      params = params ++ ["MATCH", opts[:match]]
    end

    if opts[:count] do
      params = params ++ ["COUNT", opts[:count]]
    end
    call_server(pid, {:raw, :hscan, params}) |> hash_reply
  end

  # RPUSH key value [value ...] - Append one or multiple values to a list
  @spec rpush(pid, key, []) :: int_reply
  def rpush(pid \\ nil, key, values) do
    if not is_list(values) do
      values = [values]
    end
    call_server(pid, List.to_tuple([:rpush, key] ++ values)) |> int_reply
  end

  # LINSERT key BEFORE|AFTER pivot value
  # Insert an element before or after another element in a list
  @spec linsert_opt(pid, key, :before | :after, binary, binary) :: int_reply
  def linsert_opt(pid \\ nil, key, order, pivot, value) when order in [:before, :after] do
    call_server(pid, {:raw, :linsert, [key, order, pivot, value]}) |> int_reply
  end

  # BLPOP key [key ...] timeout
  # Remove and get the first element in a list, or block until one is available
  @spec blpop(pid, [], integer) :: hash_reply
  def blpop(pid \\ nil, keys, timeout) do
    if not is_list(keys) do
      keys = [keys]
    end
    call_server(pid, List.to_tuple([:blpop] ++ keys ++ [timeout])) |> hash_reply
  end

  # BRPOP key [key ...] timeout
  # Remove and get the last element in a list, or block until one is available
  @spec brpop(pid, [], integer) :: hash_reply
  def brpop(pid \\ nil, keys, timeout) do
    if not is_list(keys) do
      keys = [keys]
    end
    call_server(pid, List.to_tuple([:brpop] ++ keys ++ [timeout])) |> hash_reply
  end

  # BRPOPLPUSH source destination timeout
  # Pop a value from a list, push it to another list and return it; or block until one is available
  @spec brpoplpush(pid, binary, binary, integer) :: sts_reply
  def brpoplpush(pid \\ nil, source, destination, timeout) do
    call_server(pid, {:brpoplpush, source, destination, timeout}) |> sts_reply
  end

  # LPUSH key value [value ...]
  # Prepend one or multiple values to a list
  @spec lpush(pid, key, []) :: int_reply
  def lpush(pid \\ nil, key, values) do
    if not is_list(values) do
      values = [values]
    end
    call_server(pid, List.to_tuple([:lpush, key] ++ values)) |> int_reply
  end


  # SADD key member [member ...] - Add one or more members to a set
  @spec sadd(pid, key, []) :: int_reply
  def sadd(pid \\ nil, key, members) do
    if not is_list(members) do
      members = [members]
    end
    call_server(pid, List.to_tuple([:sadd, key] ++ members)) |> int_reply
  end

  # SDIFF key [key ...] - Subtract multiple sets
  @spec sdiff(pid, []) :: hash_reply
  def sdiff(pid \\ nil, keys) do
    if not is_list(keys) do
      keys = [keys]
    end
    call_server(pid, List.to_tuple([:sdiff] ++ keys)) |> hash_reply
  end

  # SDIFFSTORE destination key [key ...]
  # Subtract multiple sets and store the resulting set in a key
  @spec sdiffstore(pid, destination, []) :: int_reply
  def sdiffstore(pid \\ nil, destination, keys) do
    if not is_list(keys) do
      keys = [keys]
    end
    call_server(pid, List.to_tuple([:sdiffstore, destination] ++ keys)) |> int_reply
  end

  # SINTER key [key ...] - Intersect multiple sets
  @spec sinter(pid, []) :: hash_reply
  def sinter(pid \\ nil, keys) do
    if not is_list(keys) do
      keys = [keys]
    end
    call_server(pid, List.to_tuple([:sinter] ++ keys)) |> hash_reply
  end

  # SINTERSTORE destination key [key ...]
  # Intersect multiple sets and store the resulting set in a key
  @spec sinterstore(pid, destination, []) :: int_reply
  def sinterstore(pid \\ nil, destination, keys) do
    if not is_list(keys) do
      keys = [keys]
    end
    call_server(pid, List.to_tuple([:sinterstore, destination] ++ keys)) |> int_reply
  end

  # SRANDMEMBER key [count] - Get one or multiple random members from a set

  # SREM key member [member ...] - Remove one or more members from a set
  @spec srem(pid, key, []) :: int_reply
  def srem(pid \\ nil, key, members) do
    if not is_list(members) do
      members = [members]
    end
    call_server(pid, List.to_tuple([:srem, key] ++ members)) |> int_reply
  end

  # SUNION key [key ...] -  Add multiple sets
  @spec sunion(pid, []) :: hash_reply
  def sunion(pid \\ nil, keys) do
    if not is_list(keys) do
      keys = [keys]
    end
    call_server(pid, List.to_tuple([:sunion] ++ keys)) |> hash_reply
  end

  # SUNIONSTORE destination key [key ...]
  # Add multiple sets and store the resulting set in a key
  @spec sunionstore(pid, destination, []) :: int_reply
  def sunionstore(pid \\ nil, destination, keys) do
    if not is_list(keys) do
      keys = [keys]
    end
    call_server(pid, List.to_tuple([:sunionstore, destination] ++ keys)) |> int_reply
  end

  # SSCAN key cursor [MATCH pattern] [COUNT count]
  # Incrementally iterate Set elements
  @spec sscan(pid, key, cursor) :: hash_reply
  def sscan(pid \\ nil, key, cursor), do: sscan_opt(pid, key, cursor, [])

  @spec sscan_opt(pid, key, cursor, any) :: hash_reply
  def sscan_opt(pid \\ nil, key, cursor, opts) do
    params = [key, cursor]
    if opts[:match] do
      params = params ++ ["MATCH", opts[:match]]
    end

    if opts[:count] do
      params = params ++ ["COUNT", opts[:count]]
    end
    call_server(pid, {:raw, :sscan, params}) |> hash_reply
  end

  # ZADD key score member [score member ...]
  # Add one or more members to a sorted set, or update its score if it already exists
  @spec zadd(pid, key, []) :: int_reply
  def zadd(pid \\ nil, key, kvs) do
    call_server(pid, {:raw, :zadd, [key] ++ kvs}) |> int_reply
  end

  # ZINTERSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX]
  # Intersect multiple sorted sets and store the resulting sorted set in a new key
  @spec zinterstore(pid, destination, count, keys) :: int_reply
  def zinterstore(pid \\ nil, destination, numkeys, keys), do: zinterstore_opt(pid, destination, numkeys, keys, [])

  @spec zinterstore_opt(pid, destination, count, keys, any) :: int_reply
  def zinterstore_opt(pid \\ nil, destination, numkeys, keys, opts) do
    if not is_list(keys) do
      keys = [keys]
    end
    params = [destination, numkeys] ++ keys
    if opts[:weights] do
      weights = opts[:weights]
      if not is_list(weights) do
        weights = [weights]
      end 
      params = params ++ ["WEIGHTS"] ++ weights
    end

    if opts[:aggregate] in [:sum, :min, :max] do
      params = params ++ ["AGGREGATE", opts[:aggregate]]
    end
    call_server(pid, {:raw, :zinterstore, params}) |> int_reply
  end


  # ZRANGE key start stop [WITHSCORES]
  # Return a range of members in a sorted set, by index
  @spec zrange(pid, key, start, stop) :: hash_reply
  def zrange(pid \\ nil, key, start, stop), do: zrevrange_opt(pid, key, start, stop, [])

  @spec zrange_opt(pid, key, start, stop, any) :: hash_reply
  def zrange_opt(pid \\ nil, key, start, stop, opts) do
    params = [key, start, stop]
    if opts[:withscores] do
      params = params ++ ["WITHSCORES"]
    end
    call_server(pid, {:raw, :zrange, params}) |> hash_reply
  end

  # ZRANGEBYLEX key min max [LIMIT offset count]
  # Return a range of members in a sorted set, by lexicographical range
  @spec zrangebylex(pid, key, max, min) :: hash_reply
  def zrangebylex(pid \\ nil, key, max, min), do: zrangebylex_opt(pid, key, max, min, [])

  @spec zrangebylex_opt(pid, key, max, min, any) :: hash_reply
  def zrangebylex_opt(pid \\ nil, key, max, min, opts) do
    params = [key, max, min]

    if opts[:limit] do
      limit = opts[:limit]
      params = params ++ ["LIMIT", limit[:offset], limit[:count]]
    end
    call_server(pid, {:raw, :zrangebylex, params}) |> hash_reply
  end

  # ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
  # Return a range of members in a sorted set, by score
  @spec zrangebyscore(pid, key, max, min) :: hash_reply
  def zrangebyscore(pid \\ nil, key, max, min), do: zrangebyscore_opt(pid, key, max, min, [])

  @spec zrangebyscore_opt(pid, key, max, min, any) :: hash_reply
  def zrangebyscore_opt(pid \\ nil, key, max, min, opts) do
    params = [key, max, min]
    if opts[:withscores] do
      params = params ++ ["WITHSCORES"]
    end

    if opts[:limit] do
      limit = opts[:limit]
      params = params ++ ["LIMIT", limit[:offset], limit[:count]]
    end
    call_server(pid, {:raw, :zrangebyscore, params}) |> hash_reply
  end

  # ZREM key member [member ...] - Remove one or more members from a sorted set
  @spec zrem(pid, key, []) :: int_reply
  def zrem(pid \\ nil, key, members) do
    if not is_list(members) do
      members = [members]
    end
    call_server(pid, List.to_tuple([:zrem, key] ++ members)) |> int_reply
  end

  # ZREVRANGE key start stop [WITHSCORES]
  # Return a range of members in a sorted set, by index, with scores ordered from high to low
  @spec zrevrange(pid, key, start, stop) :: hash_reply
  def zrevrange(pid \\ nil, key, start, stop), do: zrevrange_opt(pid, key, start, stop, [])

  @spec zrevrange_opt(pid, key, start, stop, any) :: hash_reply
  def zrevrange_opt(pid \\ nil, key, start, stop, opts) do
    params = [key, start, stop]
    if opts[:withscores] do
      params = params ++ ["WITHSCORES"]
    end
    call_server(pid, {:raw, :zrevrange, params}) |> hash_reply
  end

  # ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
  # Return a range of members in a sorted set, by score, with scores ordered from high to low
  @spec zrevrangebyscore(pid, key, max, min) :: hash_reply
  def zrevrangebyscore(pid \\ nil, key, max, min), do: zrevrangebyscore_opt(pid, key, max, min, [])

  @spec zrevrangebyscore_opt(pid, key, max, min, any) :: hash_reply
  def zrevrangebyscore_opt(pid \\ nil, key, max, min, opts) do
    params = [key, max, min]
    if opts[:withscores] do
      params = params ++ ["WITHSCORES"]
    end

    if opts[:limit] do
      limit = opts[:limit]
      params = params ++ ["LIMIT", limit[:offset], limit[:count]]
    end
    call_server(pid, {:raw, :zrevrangebyscore, params}) |> hash_reply
  end

  # ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]]
  # [AGGREGATE SUM|MIN|MAX]
  # Add multiple sorted sets and store the resulting sorted set in a new key
  @spec zunionstore(pid, destination, count, keys) :: int_reply
  def zunionstore(pid \\ nil, destination, count, keys), do: zunionstore_opt(pid, destination, count, keys, [])

  @spec zunionstore_opt(pid, destination, count, keys, any) :: int_reply
  def zunionstore_opt(pid \\ nil, destination, count, keys, opts) do
    if not is_list(keys) do
      keys = [keys]
    end
    params = [destination, count] ++ keys
    if opts[:weights] do
      weights = opts[:weights]
      if not is_list(weights) do
        weights = [weights]
      end 
      params = params ++ ["WEIGHTS"] ++ weights
    end

    if opts[:aggregate] in [:sum, :min, :max] do
      params = params ++ ["AGGREGATE", opts[:aggregate]]
    end
    call_server(pid, {:raw, :zunionstore, params}) |> int_reply
  end

  # ZSCAN key cursor [MATCH pattern] [COUNT count]
  # Incrementally iterate sorted sets elements and associated scores
  @spec zscan(pid, key, cursor) :: hash_reply
  def zscan(pid \\ nil, key, cursor), do: zscan_opt(pid, key, cursor, [])

  @spec zscan_opt(pid, key, cursor, any) :: hash_reply
  def zscan_opt(pid \\ nil, key, cursor, opts) do
    params = [key, cursor]
    if opts[:match] do
      params = params ++ ["MATCH", opts[:match]]
    end

    if opts[:count] do
      params = params ++ ["COUNT", opts[:count]]
    end
    call_server(pid, {:raw, :zscan, params}) |> hash_reply
  end

  # PSUBSCRIBE pattern [pattern ...]
  # Listen for messages published to channels matching the given patterns
  @spec psubscribe(pid, []) :: as_is
  def psubscribe(pid \\ nil, patterns) do
    if not is_list(patterns) do
      patterns = [patterns]
    end
    call_server(pid, List.to_tuple([:psubscribe] ++ patterns)) |> as_is
  end

  # PUBSUB subcommand [argument [argument ...]]
  # Inspect the state of the Pub/Sub subsystem
  def pubsub(pid \\ nil, command, arg) when command in [:channels, :numsub, :numpat] do
    case command do
      :channels ->
        call_server(pid, {:pubsub, :channels, arg}) |> hash_reply
      :numsub ->
        if not is_list(arg) do
          arg = [arg]
        end
        call_server(pid, List.to_tuple([:pubsub, :numsub] ++ arg)) |> hash_reply
      :numpat ->
        call_server(pid, {:pubsub, :numpat}) |> int_reply
    end
  end

  # PUNSUBSCRIBE [pattern [pattern ...]]
  # Stop listening for messages posted to channels matching the given patterns
  @spec punsubscribe(pid, []) :: as_is
  def punsubscribe(pid \\ nil, patterns) do
    if not is_list(patterns) do
      patterns = [patterns]
    end
    call_server(pid, List.to_tuple([:punsubscribe] ++ patterns)) |> as_is
  end

  # SUBSCRIBE channel [channel ...]
  # Listen for messages published to the given channels
  @spec subscribe(pid, []) :: as_is
  def subscribe(pid \\ nil, channels) do
    if not is_list(channels) do
      channels = [channels]
    end
    call_server(pid, List.to_tuple([:subscribe] ++ channels)) |> as_is
  end

  # UNSUBSCRIBE [channel [channel ...]]
  # Stop listening for messages posted to the given channels
  @spec unsubscribe(pid, []) :: as_is
  def unsubscribe(pid \\ nil, channels) do
    if not is_list(channels) do
      channels = [channels]
    end
    call_server(pid, List.to_tuple([:unsubscribe] ++ channels)) |> as_is
  end

  @spec flushall(pid) :: sts_reply
  def flushall(pid\\nil) do
    call_server(pid, {:flushall}) |> sts_reply
  end

  for {fun, args, reply} <- funs do
    args = Enum.map(args, &(quote(do: unquote(&1))))
    spec_args = [quote(do: var!(pid)) | args] |> Enum.map(&Macro.expand(&1, __ENV__)) 
    fun_args = [quote(do: var!(pid)\\nil) | args]
    msg = [fun | args]
 
    @spec unquote(fun)(unquote_splicing(spec_args)) :: unquote(reply)()
    def unquote(fun)(unquote_splicing(fun_args)) do
      call_server(pid, {unquote_splicing(msg)}) |> unquote(reply)()
    end
  end

  def multi(pid\\nil, func) do
    call_server(pid, { :multi })
    func.()
    {:ok, result} = call_server(pid, { :exec })
    Enum.map(result, &map_reply/1)
  end


  @spec call_server(pid, tuple|atom) :: value
  defp call_server(pid, args) do
    :gen_server.call(pid || client, args)
  end

  @spec client() :: pid
  defp client do
    Process.whereis(:redis)
  end
  
  @spec bool_reply(binary) :: boolean
  defp bool_reply("0"), do: false

  @spec bool_reply(binary) :: boolean
  defp bool_reply("1"), do: true

  @spec int_reply(binary) :: integer
  defp int_reply(reply) do
    reply |> String.to_integer
  end

  @spec sts_reply(binary) :: :ok | binary
  defp sts_reply("OK"), do:
    :ok

  defp sts_reply(reply), do:
    reply

  @spec as_is(binary) :: binary
  defp as_is(reply), do: reply

  @spec ok_reply(binary) :: tuple
  defp ok_reply("OK") do
    {:ok, ""}
  end
  defp ok_reply(message) do
    {:err, message}
  end

  @spec hash_reply(binary) :: binary
  defp hash_reply(reply) do
    coll = Enum.chunk(reply, 2) |> Enum.map(fn [a, b] -> {a, b} end)
    Enum.into(coll, HashDict.new)
  end

  defp map_reply("OK"), do: :ok
  defp map_reply(reply), do: reply
end
