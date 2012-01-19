# CHANGELOG

## v1.0

* Support added for pubsub. Implemented in `eredis_sub` and
  `eredis_sub_client` is a subscriber that will forward messages from
  Redis to an Erlang process with flow control. The user can configure
  to either drop messages or crash the driver if a certain queue size
  inside the driver is reached.

## v0.7.0

* Support added for pipelining requests, which allows batching
  multiple requests in a single call to eredis. Thanks to Dave
  Peticolas (jdavisp3) for the implementation.

## v0.6.0

* Support added for transactions, by Dave Peticolas (jdavisp3) who implemented
  parsing of nested multibulks.

## v0.5.0

* Configurable reconnect sleep time, by Valentino Volonghi (dialtone)

* Support for using eredis as a poolboy worker, by Valentino Volonghi
  (dialtone)