# Releasing IsoWatch

A release is two Git tags on the same commit:

| Tag | What it releases |
|---|---|
| `vX.Y.Z` | The library, `github.com/DCchoudhury15/isowatch`. This tag also starts the release workflow. |
| `processor/isowatchprocessor/vX.Y.Z` | The collector processor. Go finds a module in a subfolder by a tag that starts with the folder's path. |

`examples/bank` is not released.

The release workflow ([.github/workflows/release.yml](.github/workflows/release.yml)) then:

1. checks that the version, the processor's `go.mod`, `CHANGELOG.md` and the processor tag are right ([scripts/check-release.sh](scripts/check-release.sh)),
2. runs every CI check, including the PostgreSQL 18 tests,
3. builds the collector for Linux and macOS, on amd64 and arm64,
4. pushes the image `ghcr.io/dcchoudhury15/isowatch-collector:X.Y.Z` (and `:latest`) for linux/amd64 and linux/arm64,
5. creates the GitHub release with the archives, `checksums.txt` and the notes from `CHANGELOG.md`,
6. installs the library and the processor from the public Go proxy, and builds a collector with ocb from them, the way a user would.

Nothing is built or published unless step 1 passes, and nothing is published unless step 2 passes.

## Steps

1. Make sure CI is green on `main`.

2. In `CHANGELOG.md`, rename `## [Unreleased]` to `## [X.Y.Z] - YYYY-MM-DD` and add a new, empty `## [Unreleased]` above it. Update the links at the bottom:

   ```text
   [Unreleased]: https://github.com/DCchoudhury15/isowatch/compare/vX.Y.Z...HEAD
   [X.Y.Z]: https://github.com/DCchoudhury15/isowatch/releases/tag/vX.Y.Z
   ```

3. Make the processor require the library at the new version:

   ```bash
   (cd processor/isowatchprocessor && go mod edit -require=github.com/DCchoudhury15/isowatch@vX.Y.Z)
   ```

   Inside this repository, a `replace` line points the processor at `../..`, so this version is not downloaded here. But Go ignores the `replace` lines of a dependency. A user who adds the processor gets exactly the library version written in `require`, so it has to be the version released with it. For 0.1.0 it is already set.

4. Check:

   ```bash
   scripts/check-release.sh vX.Y.Z
   ```

5. Commit (`release: vX.Y.Z`), push to `main`, and wait for CI to pass.

6. Tag that commit and push both tags in one command:

   ```bash
   git tag -a vX.Y.Z -m "IsoWatch vX.Y.Z"
   git tag -a processor/isowatchprocessor/vX.Y.Z -m "IsoWatch processor vX.Y.Z"
   git push origin vX.Y.Z processor/isowatchprocessor/vX.Y.Z
   ```

   Do not use `git push --tags`. It pushes every local tag, including ones you forgot about.

7. Watch the release workflow on the Actions tab, or with `gh run watch`.

8. After the **first** release only: a new GHCR package can start out private. Open the package page (your GitHub profile, then Packages, then `isowatch-collector`), and under Package settings set the visibility to public. Check that it worked while logged out of GHCR:

   ```bash
   docker logout ghcr.io
   docker pull ghcr.io/dcchoudhury15/isowatch-collector:X.Y.Z
   ```

## Pre-releases

A tag like `v0.2.0-rc.1` makes a pre-release. The processor must require `v0.2.0-rc.1` too. The GitHub release is marked as a pre-release, the image gets only the `0.2.0-rc.1` tag (not `latest`), and a `CHANGELOG.md` section is optional.

## If something fails

- **The check or the tests fail.** Nothing was published, and the Go proxy has not seen the version yet (the install job runs after them). Delete both tags, fix the problem, and tag again:

  ```bash
  git push origin :refs/tags/vX.Y.Z :refs/tags/processor/isowatchprocessor/vX.Y.Z
  git tag -d vX.Y.Z processor/isowatchprocessor/vX.Y.Z
  ```

- **A job fails for an outside reason**, like a network error while pushing the image. Run the failed jobs again: `gh run rerun <run id> --failed`.

- **Anything after that.** Once the install job has run, the Go checksum database has recorded the version, and Go users would get errors if the tag moved. Never move or reuse a tag after that point. Fix the problem and release the next patch version.
