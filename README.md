# go-libp2p-collect

[![](https://img.shields.io/badge/made%20by-BDWare%20Team-red.svg?style=flat-square)](http://bdware.org/)
[![](https://img.shields.io/badge/project-libp2p-yellow.svg?style=flat-square)](http://github.com/libp2p/libp2p)
[![](https://img.shields.io/badge/freenode-%23libp2p-yellow.svg?style=flat-square)](http://webchat.freenode.net/?channels=%23libp2p)
[![Discourse posts](https://img.shields.io/discourse/https/discuss.libp2p.io/posts.svg)](https://discuss.libp2p.io)
[![Build Status](https://travis-ci.com/BDWare/go-libp2p-collect.svg?branch=master)](https://travis-ci.com/BDWare/go-libp2p-collect)

> A pub-sub-collect system.

This is the a pub-sub-collect implementation for libp2p.

We currently provide following implementations:
- floodcollect, which is based on floodpub——the baseline flooding protocol.

## Table of Contents

- [Install](#install)
- [Usage](#usage)
- [Documentation](#documentation)
- [Tracing](#tracing)
- [Contribute](#contribute)
- [License](#license)

## Install

```
go get github.com/bdware/go-libp2p-collect
```

## Usage

To be used for messaging and data collection in p2p instrastructure (as part of libp2p) such as BDWare, IPFS, Ethereum, other blockchains, etc.

## Documentation

See [API documentation](https://pkg.go.dev/github.com/bdware/go-libp2p-collect).

## Contribute

Contributions welcome. Please check out [the issues](https://github.com/BDWare/go-libp2p-collect/issues).

Small note: If editing the README, please conform to the [standard-readme](https://github.com/RichardLitt/standard-readme) specification.

## License

[MIT](LICENSE)

Copyright (c) 2020 The BDWare Authors. All rights reserved.
