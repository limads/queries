mkdir -p vendored 

cargo vendor vendored/deps

cd vendored/deps

unused=(
    "winapi-x86_64-pc-windows-gnu" "winapi-i686-pc-windows-gnu" "winapi-build" "winapi-util" "windows_i686_gnu"  
    "windows_x86_64_gnu" "windows-sys" "windows_aarch64_msvc" "windows_x86_64_msvc" 
    "windows_i686_msvc" "winapi" "wasi" "wasm-bindgen" "wasm-bindgen-backend" "wasm-bindgen-macro"
    "wasm-bindgen-macro-support" "wasm-bindgen-shared" "vcpkg"
)

for i in "${unused[@]}"
do 
  rm -rf "$i"
done

cd ../..

tar -czvf vendored.tar.gz vendored

rm -rf vendored

SHA=$(sha256sum vendored.tar.gz)

echo "Vendored deps exported (${SHA})"
