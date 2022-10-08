PKG_CONFIG_PATH=/app/lib:/app/lib/pkgconfig
ICON_SRC=/run/build/Queries/data/icons/hicolor
ICON_DST=/app/share/icons/hicolor
FLATPAK_ICON_DST=/app/share/app-info/icons/flatpak
CARGO_TARGET_PATH=/run/build/Queries/target/release

mkdir -p ${ICON_DST}/scalable/apps ${ICON_DST}/symbolic/apps ${FLATPAK_ICON_DST}/64x64 ${FLATPAK_ICON_DST}/128x128
install -D ${ICON_SRC}/scalable/apps/${FLATPAK_ID}.svg ${ICON_DST}/scalable/apps
install -D ${ICON_SRC}/symbolic/apps/${FLATPAK_ID}-symbolic.svg ${ICON_DST}/symbolic/apps
install -D ${ICON_SRC}/64x64/apps/${FLATPAK_ID}.png ${FLATPAK_ICON_DST}/64x64
install -D ${ICON_SRC}/128x128/apps/${FLATPAK_ID}.png ${FLATPAK_ICON_DST}/128x128
mkdir -p ${FLATPAK_DEST}/share/applications ${FLATPAK_DEST}/share/glib-2.0/schemas
install -D ${FLATPAK_BUILDER_BUILDDIR}/data/${FLATPAK_ID}.desktop ${FLATPAK_DEST}/share/applications
install -D ${FLATPAK_BUILDER_BUILDDIR}/data/${FLATPAK_ID}.gschema.xml ${FLATPAK_DEST}/share/glib-2.0/schemas
mkdir -p ${FLATPAK_DEST}/share/appdata ${FLATPAK_DEST}/share/app-info/xmls
gzip ${FLATPAK_BUILDER_BUILDDIR}/data/${FLATPAK_ID}.appdata.xml --keep
install -D ${FLATPAK_BUILDER_BUILDDIR}/data/${FLATPAK_ID}.appdata.xml.gz ${FLATPAK_DEST}/share/app-info/xmls
mkdir -p ${FLATPAK_DEST}/bin
install -D ${CARGO_TARGET_PATH}/queries ${FLATPAK_DEST}/bin
