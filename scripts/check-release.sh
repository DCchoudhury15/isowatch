#!/usr/bin/env bash
# Checks that a version tag is ready to release, before anything is built or
# published. The release workflow runs it; you can run it before tagging:
#
#   scripts/check-release.sh v0.1.0
#
# 1. The tag looks like vMAJOR.MINOR.PATCH, optionally with -pre.release.
# 2. processor/isowatchprocessor/go.mod requires the root module at this
#    version. Go ignores the replace line for users of the processor, so a
#    wrong version here would give them a different library than the one
#    the processor was tested with.
# 3. CHANGELOG.md has a "## [x.y.z]" section (not needed for pre-releases).
# 4. With --remote: the processor tag is on origin and on the same commit.
set -euo pipefail

usage() {
  echo "usage: $0 vX.Y.Z [--remote]" >&2
  exit 2
}

tag="${1:-}"
[ -n "$tag" ] || usage
remote=false
if [ "${2:-}" = "--remote" ]; then
  remote=true
elif [ -n "${2:-}" ]; then
  usage
fi

cd "$(dirname "$0")/.."
fail=0

if ! [[ "$tag" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[0-9A-Za-z.-]+)?$ ]]; then
  echo "Tag $tag is not a version like v0.1.0 or v0.2.0-rc.1."
  exit 1
fi
version="${tag#v}"

want="github.com/DCchoudhury15/isowatch $tag"
have="$(cd processor/isowatchprocessor && go list -m -f '{{.Path}} {{.Version}}' github.com/DCchoudhury15/isowatch)"
if [ "$have" != "$want" ]; then
  echo "processor/isowatchprocessor/go.mod requires \"$have\", want \"$want\"."
  echo "Fix it with: (cd processor/isowatchprocessor && go mod edit -require=github.com/DCchoudhury15/isowatch@$tag)"
  fail=1
fi

case "$version" in
  *-*) ;;
  *)
    if ! grep -qF "## [$version]" CHANGELOG.md; then
      echo "CHANGELOG.md has no \"## [$version]\" section."
      fail=1
    fi
    ;;
esac

if $remote; then
  ptag="processor/isowatchprocessor/$tag"
  # The last line is the commit, also for an annotated tag (its ^{} line).
  at="$(git ls-remote --tags origin "refs/tags/$ptag" "refs/tags/$ptag^{}" | tail -n 1 | cut -f 1)"
  if [ -z "$at" ]; then
    echo "Tag $ptag is not on origin. Push both tags together (see RELEASING.md)."
    fail=1
  elif [ "$at" != "$(git rev-parse HEAD)" ]; then
    echo "Tag $ptag is on commit $at, but $tag is on $(git rev-parse HEAD)."
    fail=1
  fi
fi

if [ "$fail" -ne 0 ]; then
  exit 1
fi
echo "$tag is ready to release."
