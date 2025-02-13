# go-msquic

`go-msquic` is a Go wrapper for Microsoft's QUIC (MsQuic) library, providing Go developers with an easy interface to work with QUIC-based protocols such as HTTP/3.

## Prerequisite

### Local MsQuic Build

Before all, it is highly recommended to build MsQuic locally.
This library uses CGO and need both C headers & the library (shared or static, see below).
You can get MsQuic via:

```
git clone https://github.com/microsoft/msquic
```

For a static build (libmsquic.a), do the following:

```
mkdir build-static
cd build-static
cmake .. -DCMAKE_BUILD_TYPE=MinSizeRel -DQUIC_BUILD_SHARED=OFF
```

For a dynamic build (libmsquic.so), do the following:

```
mkdir build
cd build
cmake .. -DCMAKE_BUILD_TYPE=MinSizeRel
```


### Set the right package-config

Package-config is the tool used by CGO to find the headers & library automatically.

You can find package-config (.pc) file in this repository. Copy the following `pcs/msquic-static.pc` (for a static build) or `pcs/msquic.pc` (for a dynamic build) into `/usr/share/pkgconfig/msquic.pc` - or anywhere accessible by pkg-config tool.

## Wrapper Installation

To install `go-msquic`, ensure you have Go installed on your system. Then run the following command to install the package:

```bash
go get github.com/noboruma/go-msquic

```
If the prerequisites were done properly, you can now compile your project with `CGO_ENABLED=1` and start using msquic.

## License

MIT License
