module Turbofan
  module Extensions
    PLATFORM = "linux_arm64"

    COMMUNITY = %i[h3 delta].freeze

    CORE_REPO = "https://extensions.duckdb.org"
    COMMUNITY_REPO = "https://community-extensions.duckdb.org"
    private_constant :PLATFORM, :COMMUNITY, :CORE_REPO, :COMMUNITY_REPO

    def self.version
      "v#{Turbofan.config.duckdb_version}"
    end

    def self.repo_url(ext)
      repo = COMMUNITY.include?(ext.to_sym) ? COMMUNITY_REPO : CORE_REPO
      "#{repo}/#{version}/#{PLATFORM}/#{ext}.duckdb_extension.gz"
    end

    def self.install_path
      "/root/.duckdb/extensions/#{version}/#{PLATFORM}"
    end
  end
end
