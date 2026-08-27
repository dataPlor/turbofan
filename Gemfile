source "https://rubygems.org"

gemspec

group :development, :test do
  gem "rspec", "~> 3"
  gem "rexml"
  gem "rubocop", "~> 1", require: false
  # rubocop's transitive dep. parallel 2.x requires Ruby >= 3.3, which would drop the
  # 3.2 test job -- but the gem must keep running on 3.2: every Batch step container is
  # FROM amazonlinux:2023, whose ruby is 3.2 (see the ruby3.2-gems GEM_PATH in the step
  # Dockerfiles). Constrain the linter's dep rather than the supported runtime.
  gem "parallel", "< 2.0"
end
