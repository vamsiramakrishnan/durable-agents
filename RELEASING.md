# Releasing Tape

Tape ships as five artifacts that all share one version tag:

| Artifact      | Where                                                       | Built by                                          |
|---------------|-------------------------------------------------------------|---------------------------------------------------|
| `tape-server` | GitHub Releases (`.tar.gz` per `{os, arch}`)                | `.github/workflows/release.yml`                   |
| `tape-py`     | [PyPI](https://pypi.org/project/tape-py/)                   | `release.yml::python` (gated)                     |
| `tape-cli`    | [PyPI](https://pypi.org/project/tape-cli/)                  | `release.yml::python` (gated)                     |
| `tape-ts`     | [npm](https://www.npmjs.com/package/tape-ts)                | `release.yml::npm` (gated)                        |
| `tape-java`   | [Maven Central](https://central.sonatype.com/) (`dev.tape`) | `release.yml::maven` (gated)                      |

The Rust binary publishes unconditionally. The four language packages are gated
on **secret existence** — they activate automatically the moment the
corresponding secret is set in repo settings; you don't have to edit the
workflow. Until then, only `tape-server` ships, and `install.sh` falls back to
the build-from-source path automatically.

## One-time setup

Set these in **Settings → Secrets and variables → Actions** (you only need the
language packages you actually intend to publish):

| Secret                                | Enables job              | How to get                                                   |
|---------------------------------------|--------------------------|---------------------------------------------------------------|
| `PYPI_API_TOKEN`                      | `python` (PyPI publish)  | <https://pypi.org/manage/account/token/> — scope to `tape-py` + `tape-cli` |
| `NPM_TOKEN`                           | `npm` (npm publish)      | <https://www.npmjs.com/settings/~/tokens> — `automation` token |
| `MAVEN_USERNAME` + `MAVEN_PASSWORD`   | `maven` (Sonatype OSSRH) | <https://central.sonatype.org/publish/publish-portal-token/> |
| `GPG_PRIVATE_KEY` + `GPG_PASSPHRASE`  | `maven` (artifact signing) | `gpg --armor --export-secret-keys YOUR_KEY_ID`             |

The Java publish also needs a `<distributionManagement>` block and a `<profile id="release">` in `tape/sdk/java/pom.xml` configured to your OSSRH staging URL — add those when you wire Maven publishing.

## Cutting a release

```bash
# 1. Decide the version and bump every manifest in lockstep.
VER=0.1.0
sed -i "s/^version = .*/version = \"$VER\"/" tape/sdk/python/pyproject.toml tape/cli/pyproject.toml
sed -i "s/\"version\": \".*\"/\"version\": \"$VER\"/" tape/sdk/typescript/package.json
sed -i "s|<version>0\\..*</version>|<version>$VER</version>|" tape/sdk/java/pom.xml
sed -i "s/^version = .*/version = \"$VER\"/" tape/server/Cargo.toml

# 2. Update the changelog.
$EDITOR CHANGELOG.md

# 3. Commit + tag.
git add -A
git commit -m "release: v$VER"
git tag "v$VER" -m "v$VER"
git push origin main "v$VER"
```

Pushing the tag triggers `.github/workflows/release.yml`. The Rust binary
matrix runs first; the four language jobs run conditionally on their secret.

## Watching the release

```bash
# CI status
gh run watch              # or the Actions tab

# Once green
gh release view "v$VER" --web
```

`install.sh` will resolve `latest` and start serving the new version
immediately. If you tagged but the language jobs were skipped (no secret set),
the binary still ships and the build-from-source path keeps working — no
broken state.

## Backing out

`git tag -d v$VER && git push origin :refs/tags/v$VER` removes the tag. GitHub
keeps the release object until you delete it manually
(`gh release delete v$VER`). PyPI and npm don't allow re-uploading the same
version, so the workflow is "bump the patch and re-cut" — never re-publish.

## Versioning

Tape uses semver. Pre-1.0, the contract is:

- **patch** (`0.1.x`) — bug fixes, additive APIs, doc-only changes.
- **minor** (`0.x.0`) — new SDK surface, new proto messages, new connectors / sinks. Backwards-compatible on the wire.
- **major** (`x.0.0`) — wire-protocol breakages; we'll bump to `1.0` before that's even on the table.

The wire protocol (`tape/proto/tape.proto`) is the contract. Anything additive
to it can land in a minor; anything that renumbers a field or removes an RPC
is a major.
