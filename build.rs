use glib_build_tools;

fn main() {
    glib_build_tools::compile_resources(
        "data/resources",
        "data/resources/resources.gresource.xml",
        "compiled.gresource",
    );
}



