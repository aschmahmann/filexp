filexp
=======================

> Explore filecoin state

## Installation

Install Go and run:

```
git clone https://github.com/aschmahmann/filexp
cd filexp
git submodule update --init
make -C extern/filecoin-ffi
go install
```

Note: Currently does not work on Windows

## Running

Help instructions are in the binary. However, if you're showing up here as someone trying to explore all 1 threshold
multisigs controlled by a single wallet run `filexp msig-coins --trust-chainlove f1<the-address>`

## Contributing

Contributions are welcome!

## License

[SPDX-License-Identifier: Apache-2.0 OR MIT](LICENSE.md)
