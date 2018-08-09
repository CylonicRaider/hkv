# hkv

In-memory hierarchical key-value store.

## What is this?

`hkv` is an all-in-one-source-file library implementing the paradigm of a
key-value store, with the twist that values can be nested key-value stores
rather than "just" values (hearkening to the Zen of Python, "Namespaces are
one honking great idea -- let's do more of those!"). Transparent remote
capabilities are provided.

## How do I use it?

A typical setup would be to run the module as a server and to use the library
in client programs to store data on the server.

### Installation

Run

    pip install git+https://github.com/CylonicRaider/hkv

to install the newest version.

### Running an own server

After installing the module, you can run

    python -m hkv -l

to run an instance of the included server. See the output of the `--help`
option for more details.

### Documentation

Use the *pydoc* tool of your choice to browse the inline documentation of the
`hkv` module.
