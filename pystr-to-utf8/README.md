By default, when compiling, we don't re-generate the `.h` and `.pxd` files.
This is to speed up compile time.

If you've updated the API, regenerate them by running:

```
$ cargo clean
$ cargo build --features cbindgen
```

Then make sure to commit the updated generated files.
