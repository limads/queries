ICON_SRC=/run/build/Queries/data/icons/hicolor
ICON_DST=/app/share/icons/hicolor
CARGO_TARGET_PATH=/run/build/Queries/target/release

install -D ${ICON_SRC}/scalable/apps/${FLATPAK_ID}.svg ${ICON_DST}/scalable/apps/${FLATPAK_ID}.svg
install -D ${ICON_SRC}/symbolic/apps/${FLATPAK_ID}-symbolic.svg ${ICON_DST}/symbolic/apps/${FLATPAK_ID}-symbolic.svg
install -D ${FLATPAK_BUILDER_BUILDDIR}/data/${FLATPAK_ID}.desktop ${FLATPAK_DEST}/share/applications/${FLATPAK_ID}.desktop
install -D ${FLATPAK_BUILDER_BUILDDIR}/data/${FLATPAK_ID}.appdata.xml ${FLATPAK_DEST}/share/metainfo/${FLATPAK_ID}.appdata.xml
install -D ${CARGO_TARGET_PATH}/queries ${FLATPAK_DEST}/bin/queries
