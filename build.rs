
fn main() {
    glib_build_tools::compile_resources(
        "data/resources",
        "data/resources/resources.gresource.xml",
        "compiled.gresource",
    );
    println!("cargo:rustc-link-lib=gvc");
    println!("cargo:rustc-link-lib=cgraph");
    println!("cargo:rustc-link-lib=cdt");
    // -lgvc -lcgraph -lcdt
}



