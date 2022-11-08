# Host toolchain build:

Queries can be built on Linux in a single `cargo build`:

```
git clone https://github.com/limads/queries.git
cd queries
cargo build
```

Which will output a single executable at `target/debug/queries`
that can be used to run the application. All application resources (icons, CSS styles) 
are embedded in the binary, which makes it possible to run it from anywhere in your computer.

You must have network access for Cargo to pull the Rust dependencies during the build stage, 
and you must have the following system dependencies
previously installed:

```
gtk-4 (>=4.5)
gtksourceview5 (>=5.3.0)
libadwaita (>=0.1)
```

# Flatpak toolchain build:

The flatpak build is slightly different, since Flathub forbids network
access during the build stage. That means all dependencies must be vendored,
and pulled before the build starts. The vendored dependencies and are pulled automatically by
`flatpak-builder` into a `deps` folder locted at the root of the repository.

To build and install Queries on your system using Flatpak, see an example of 
invocation for `flatpak-builder` at `scripts/install.sh`. This command call will create a `build` 
directory and install the Queries distribution that is shipped by Flathub.
The cargo invocation used by `flatpak-builder` is described at `scripts/build.sh`.
