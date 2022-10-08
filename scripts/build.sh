cargo build --release --offline --verbose \
    --config "source.vendored-sources.directory=\"deps\"" \
    --config "source.crates-io.replace-with = \"vendored-sources\"" \

