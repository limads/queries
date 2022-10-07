# This is required for vendored git dependencies.
cargo generate-lockfile --offline

GIT_PREFIX=https://github.com/limads

cargo build --release --offline --verbose \
    --config "source.vendored-sources.directory=\"queries-deps\"" \
    --config "source.crates-io.replace-with = \"vendored-sources\"" \
    --config "source.\"${GIT_PREFIX}/archiver.git\".git = \"${GIT_PREFIX}/archiver.git\"" \
    --config "source.\"${GIT_PREFIX}/archiver.git\".replace-with = \"vendored-sources\"" \

# [source.crates-io]
# replace-with = "vendored-sources"

# [source."https://github.com/limads/archiver.git"]
# git = "https://github.com/limads/archiver.git"
# replace-with = "vendored-sources"

# [source."https://github.com/limads/papyri.git"]
# git = "https://github.com/limads/papyri.git"
# replace-with = "vendored-sources"

# [source."https://github.com/limads/stateful.git"]
# git = "https://github.com/limads/stateful.git"
# replace-with = "vendored-sources"

# [source.vendored-sources]
# directory = "queries-deps"

