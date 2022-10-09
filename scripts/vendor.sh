mkdir -p vendored 

rm Cargo.lock

cargo vendor vendored/deps

tar -czvf vendored.tar.gz vendored

rm -rf vendored

SHA=$(sha256sum vendored.tar.gz)

echo "Vendored deps exported (${SHA})"
