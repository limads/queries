mkdir -p build
flatpak-builder --repo=build/repo build/build com.github.limads.Queries.json --state-dir=build/state --force-clean --install --user
