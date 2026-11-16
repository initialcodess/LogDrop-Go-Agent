// Package logdropredis traces github.com/redis/go-redis/v9 clients for LogDrop: one span per command, named by the
// command (SET, GET, INCR), and one per pipeline. Keys, values and arguments are never recorded. The connection
// set-up of the client (dialing, HELLO, CLIENT SETINFO, AUTH) is left out, so it does not show up as endpoints in APM.
// Tracing starts with logdrop.Start.
//
//	rdb := redis.NewClient(&redis.Options{Addr: "redis:6379"})
//	logdropredis.Instrument(rdb)
package logdropredis

import (
	"strings"

	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
)

// setUp reports the commands a client sends when it opens a connection.
func setUp(cmd redis.Cmder) bool {
	switch strings.ToLower(cmd.Name()) {
	case "auth", "hello", "client", "select", "readonly":
		return true
	}
	return false
}

// Instrument traces client (a *redis.Client, *redis.ClusterClient, *redis.Ring or redis.UniversalClient).
func Instrument(client redis.UniversalClient) error {
	return redisotel.InstrumentTracing(client,
		redisotel.WithDBStatement(false),
		redisotel.WithDialFilter(true),
		redisotel.WithCommandFilter(setUp),
		redisotel.WithCommandsFilter(func(cmds []redis.Cmder) bool {
			for _, cmd := range cmds {
				if !setUp(cmd) {
					return false
				}
			}
			return true
		}))
}
