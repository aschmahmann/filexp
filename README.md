filexp
=======================

> Explore filecoin state

## Installation

There are two flavors of filexp that you can install:
1. Fully featured, including various commands for interacting with the FVM and FEVM
2. A more limited version that handles most commands (currently everything but `fevm-exec` and `fevm-daemon`)
   - This version is the only one that works on Windows

### Fully featured

Install Go and run:

```
git clone https://github.com/aschmahmann/filexp
cd filexp
git submodule update --init
make -C extern/filecoin-ffi
go install -tags fvm
```

### Limited

Install Go and run:
```
git clone https://github.com/aschmahmann/filexp
cd filexp
go install
```

## Running

Help instructions are in the binary. However, if you're showing up here as someone trying to explore all 1 threshold
multisigs controlled by a single wallet run `filexp msig-coins --trust-chainlove f1<the-address>`

## Contributing

Contributions are welcome!

## License

[SPDX-License-Identifier: Apache-2.0 OR MIT](LICENSE.md)
