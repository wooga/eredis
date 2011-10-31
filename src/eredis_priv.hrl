-include("eredis.hrl").

-record(state, {
          host :: string() | undefined,
          port :: integer() | undefined,
          password :: binary() | undefined,
          database :: binary() | undefined,
          reconnect_sleep :: integer() | undefined,

          socket :: port() | undefined,
          parser_state :: #pstate{} | undefined,
          queue :: queue() | undefined,
          subscribers :: list({reference(), pid()})
}).
