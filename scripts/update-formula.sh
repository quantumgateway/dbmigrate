#!/bin/bash
# Update Homebrew formula with new version and SHA256
# Pushes to the homebrew-tap repository

set -e

VERSION="${1:-}"

if [ -z "$VERSION" ]; then
    echo "Usage: $0 <version>"
    echo "Example: $0 v1.0.1"
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
TAP_REPO="git@github.com:quantumgateway/homebrew-tap.git"
TAP_DIR="${PROJECT_DIR}/.homebrew-tap"
REPO_URL="https://github.com/quantumgateway/dbmigrate"
TARBALL_URL="${REPO_URL}/archive/refs/tags/${VERSION}.tar.gz"

echo "==> Fetching SHA256 for ${VERSION}..."
SHA256=$(curl -sL "$TARBALL_URL" | shasum -a 256 | cut -d' ' -f1)

if [ -z "$SHA256" ] || [ "$SHA256" = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" ]; then
    echo "Error: Could not fetch tarball or tag doesn't exist"
    echo "Make sure tag ${VERSION} exists on GitHub"
    exit 1
fi

echo "    SHA256: $SHA256"

# Clone or update the tap repository
echo "==> Updating homebrew-tap repository..."
if [ -d "$TAP_DIR" ]; then
    git -C "$TAP_DIR" fetch origin
    git -C "$TAP_DIR" reset --hard origin/main
else
    git clone "$TAP_REPO" "$TAP_DIR"
fi

# Ensure Formula directory exists in tap
mkdir -p "${TAP_DIR}/Formula"

# Update the formula file
FORMULA_FILE="${TAP_DIR}/Formula/dbmigrate.rb"

cat > "$FORMULA_FILE" << EOF
class Dbmigrate < Formula
  desc "Database migration tool for ClickHouse"
  homepage "https://github.com/quantumgateway/dbmigrate"
  url "${TARBALL_URL}"
  sha256 "${SHA256}"
  license "MIT"

  depends_on "go" => :build

  def install
    system "go", "build", *std_go_args(ldflags: "-s -w -X main.Version=${VERSION}")
  end

  test do
    assert_match "dbmigrate", shell_output("#{bin}/dbmigrate --help 2>&1")
  end
end
EOF

echo "==> Updated formula:"
cat "$FORMULA_FILE"

# Commit and push
echo ""
echo "==> Committing and pushing to homebrew-tap..."
git -C "$TAP_DIR" add Formula/dbmigrate.rb
git -C "$TAP_DIR" commit -m "dbmigrate ${VERSION}"
git -C "$TAP_DIR" push origin main

echo ""
echo "==> Done! Users can now install with:"
echo "    brew tap quantumgateway/tap"
echo "    brew install dbmigrate"
