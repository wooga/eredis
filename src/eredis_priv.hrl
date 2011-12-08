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

          % The process we send pubsub and connection state messages to.
          controlling_process :: undefined | {reference(), pid()},

          % This is the queue of messages to send to the controlling
          % process.
          msg_queue :: queue(),

          % The msg_state keeps track of whether we are waiting
          % for the controlling process to acknowledge the last
          % message.
          msg_state = ready :: ready | need_ack,

          % The conn_state keeps track of whether we have set the
          % tcp connection to {active, once} after receiving some
          % tcp data.
          conn_state = passive :: passive | active_once
}).
