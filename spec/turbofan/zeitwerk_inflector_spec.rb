# frozen_string_literal: true

require "spec_helper"

# Phase 0.6.1 Zeitwerk hygiene spec (Xavier Noria's final-review ask).
#
# The `Turbofan.loader` is configured in lib/turbofan.rb with a small
# list of custom inflector rules (asl → ASL, cli → CLI, cloudformation →
# CloudFormation). If a future contributor adds a file whose basename
# matches a known acronym (SNS, IAM, ECR, DAG, ...) and forgets to
# update the inflector map, the failure won't surface in the static
# spec suite — it only raises at `loader.eager_load(force: true)` in
# spec_helper on a cold boot. This spec walks every managed file
# explicitly so the breakage is caught in any spec run.
#
# How it works:
#   1. Ask the loader for its complete file→constant map
#      (Turbofan.loader.all_expected_cpaths).
#   2. For each entry, attempt to resolve the expected constant.
#   3. Fail fast if any resolution misses.
#
# Excluded: files the loader explicitly ignores (errors.rb,
# chunking_handler.rb) and the root lib/turbofan directory itself.
RSpec.describe "Turbofan Zeitwerk inflector completeness" do
  it "resolves every managed file to its expected constant" do
    unresolved = []

    Turbofan.loader.all_expected_cpaths.each do |path, expected_cpath|
      # Skip the loader's own root-dir entries (they map to Object,
      # Turbofan itself, etc.) and any explicitly-ignored paths.
      next if expected_cpath == "Object"
      next if expected_cpath == "Turbofan" # root .rb and dir
      next if File.directory?(path) # directory entries without an owning file

      begin
        Object.const_get(expected_cpath)
      rescue NameError => e
        unresolved << {path: path, expected: expected_cpath, error: e.message}
      end
    end

    expect(unresolved).to be_empty,
      "Zeitwerk expects file → constant mappings that don't resolve. " \
      "Either the file doesn't define the expected constant, or the " \
      "loader needs an inflector rule for this acronym:\n" +
      unresolved.map { |u| "  #{u[:path]} → #{u[:expected]} (#{u[:error]})" }.join("\n")
  end
end
