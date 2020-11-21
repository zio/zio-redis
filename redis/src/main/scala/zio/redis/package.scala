package zio

package object redis
    extends api.Connection
    with api.Geo
    with api.Hashes
    with api.HyperLogLog
    with api.Keys
    with api.Lists
    with api.Sets
    with api.Strings
    with api.SortedSets
    with api.PubSub
    with options.Geo
    with options.Keys
    with options.Shared
    with options.SortedSets
    with options.Strings
    with options.Lists
    with Interpreter
