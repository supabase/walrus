# Realtime Transport

Realtime Transport reads JSON form stdin and forwards it to supabase realtime

See parent directory for example usage

## CLI

```
realtime 0.1.0
reads JSON from stdin and forwards it to supabase realtime

USAGE:
    realtime [OPTIONS]

OPTIONS:
    -h, --help                       Print help information
        --header <HEADER>=<VALUE>
        --topic <TOPIC>              [default: room:test]
        --url <URL>                  [default: wss://sendwal.fly.dev/socket]
    -V, --version                    Print version information

```