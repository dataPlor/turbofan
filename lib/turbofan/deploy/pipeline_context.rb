# frozen_string_literal: true

module Turbofan
  module Deploy
    # Consolidates the 4-line pipeline-loading preamble used across CLI
    # commands (Check, Deploy, Status). Returns the existing PipelineLoader
    # ::LoadResult struct unchanged — just hides the turbofans_root +
    # pipeline-file path construction behind a named kwarg.
    #
    # Before:
    #   turbofans_root = "turbofans"
    #   pipeline_file = File.join(turbofans_root, "pipelines", "#{name}.rb")
    #   load_result = Turbofan::Deploy::PipelineLoader.load(pipeline_file,
    #                                                       turbofans_root: turbofans_root)
    #
    # After:
    #   load_result = Turbofan::Deploy::PipelineContext.load(pipeline_name: name)
    module PipelineContext
      DEFAULT_ROOT = "turbofans".freeze

      def self.load(pipeline_name:, turbofans_root: DEFAULT_ROOT)
        pipeline_file = File.join(turbofans_root, "pipelines", "#{pipeline_name}.rb")
        PipelineLoader.load(pipeline_file, turbofans_root: turbofans_root)
      end
    end
  end
end
