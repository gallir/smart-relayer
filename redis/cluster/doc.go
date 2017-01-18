// This module implement a redis-cluster proxy, inspired by
// breadis https://github.com/mediocregopher/breadis
// As breadis, it acts as a single proxy that allows to interact
// very easily with redis-clusters.
//
// It uses the radix2 libreries (from the same breadis' author) although
// it adds interesting features.
//
// As the redis and redis2 implementations it allows to work
// in normal mode ("sync") or asynchronously for selected commands.
// The other important feature is snappy compression (option "compress = true")
// transparently to the client or server.
//
// Finally, "parallel = true" allows to create several senders for the
// same client's connections. Use it with caution, although it tries hard
// to avoid out-of-order responses, still the result of a GET can come before
// than a previous related SET is processed and therefore the result may by
// (wrongly) nil or an older value. I included and left it because it's no so expensive
// and can accelerate (a lot) operation from clients that store and/or read
// a lot of data.
//
// One more thing: there is mode "redis-plus" that connects to a single redis
// instance but using the same functions and it has the same limitations as a
// redis-cluster, i.e. all operations must have a key and SELECT is not allowed.

package cluster
