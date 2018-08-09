# hkv

In-memory hierarchical key-value store.

## What is this?

`hkv` is an all-in-one-source-file library implementing the paradigm of a
key-value store, with the twist that values can be nested key-value stores
rather than "just" values (hearkening to the Zen of Python, "Namespaces are
one honking great idea -- let's do more of those!"). Transparent remote
capabilities are provided.

## How do I use it?

A typical setup would be to run the module as a server (`python -m hkv -l`)
and to use the library in client programs to store data at the server. See the
extensive inline documentation (`pydoc hkv`, if you have installed the
library) for concrete details.
