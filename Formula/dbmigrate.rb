class Dbmigrate < Formula
  desc "Database migration tool for ClickHouse"
  homepage "https://github.com/quantumgateway/dbmigrate"
  url "https://github.com/quantumgateway/dbmigrate/archive/refs/tags/v1.0.1.tar.gz"
  sha256 "9a3653bd9163d1340945b9b0d9c64a9dab8e3bd19d58255466daa1c28032d1a6"
  license "MIT"

  depends_on "go" => :build

  def install
    system "go", "build", *std_go_args(ldflags: "-s -w")
  end

  test do
    assert_match "Usage:", shell_output("#{bin}/dbmigrate --help", 1)
  end
end
