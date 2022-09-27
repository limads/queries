use gtk4::gio;

fn main() {
    gio::compile_resources(
        "data/resources",
        "data/resources/resources.gresource.xml",
        "compiled.gresource",
    );
}


