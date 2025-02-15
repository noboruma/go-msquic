# Sample

The sample contains a simple server and client code.

## Generate TLS key/cert

The server requires a certificate to work. On linux generate it via:
```
openssl req  -nodes -new -x509  -keyout /tmp/server.key -out /tmp/server.cert
```

## Run the server

```
go run server.go
```

## Run the client

```
go run client.go
```

## Workflow

The server waits for a QUIC connection to be established. Both the client and the server then communicate via two streams. On the first stream, the client sends "Hello" and the server replies back "Bye". On the second stream, the server sends "Hello" and the client replies back "Bye". Once both stream workflows are done, the client goes away. You need to manually terminate the server process.
