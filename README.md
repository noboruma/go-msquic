# go-msquic

`go-msquic` is a Go wrapper for the [Microsoft's QUIC library](https://github.com/microsoft/msquic), providing Go developers with an easy interface to work with QUIC-based protocols such as HTTP/3.
`go-msquic` API is inspired from [quic-go](https://github.com/quic-go/quic-go) and can be used as a drop-in replacement. Unless you are ready to deal with C libraries, we do actually recommend `quic-go` over `go-msquic`.

## Prerequisite

### Local MsQuic Build

Before all, it is necessary to have a local `MsQuic` C library build.
`go-msquic` relies on CGO and needs both C headers & the library (shared or static, see below).

- First, get `MsQuic` via:

```
git clone https://github.com/microsoft/msquic
cd msquic
git submodule update --init --recursive
```

- For a static build (libmsquic.a), do the following:

```
mkdir build-static
cd build-static
cmake .. -DCMAKE_BUILD_TYPE=MinSizeRel -DQUIC_BUILD_SHARED=OFF
```

- Alternatively, for a dynamic build (libmsquic.so), do the following:

```
mkdir build
cd build
cmake .. -DCMAKE_BUILD_TYPE=MinSizeRel
```

- Then proceed with the compilation

```
make
```

### Set the right package-config

Package-config is the tool used by CGO to find the headers & library automatically.

You can find package-config (.pc) file in this repository. Copy the following `pcs/msquic-static.pc` (for a static build) or `pcs/msquic.pc` (for a dynamic build) into `/usr/share/pkgconfig/msquic.pc` - or anywhere accessible by pkg-config tool.

## Wrapper Installation

To install `go-msquic`, ensure you have Go installed on your system. Then run the following command to install the package:

```bash
go get github.com/noboruma/go-msquic
```

And use in code via:
```
import "github.com/noboruma/go-msquic/pkg/quic"
```
If the prerequisites are done properly, you can now compile your Go project relying on `go-msquic` with `CGO_ENABLED=1` and start using msquic.

## Example

There is a client & server code in the [`sample/`](https://github.com/noboruma/go-msquic/tree/main/sample) directory.

## License

MIT License
