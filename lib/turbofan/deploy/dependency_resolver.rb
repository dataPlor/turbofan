# frozen_string_literal: true

require "fileutils"
require "pathname"
require "rbconfig"
require "tmpdir"

module Turbofan
  module Deploy
    module DependencyResolver
      STDLIB_PREFIXES = %w[rubylibdir archdir sitelibdir sitearchdir vendorlibdir vendorarchdir]
        .map { |k| RbConfig::CONFIG[k] }
        .compact
        .freeze

      # Resolve external deps for each step by loading worker.rb in a fork.
      #
      # project_root is added to $LOAD_PATH in the fork so that workers
      # using `require "services/foo"` resolve correctly during detection.
      #
      # Returns Hash<Symbol, Array<String>> — step_name => [absolute paths]
      def self.resolve(step_dirs, project_root: Dir.pwd)
        result = {}
        step_dirs.each do |step_name, step_dir|
          worker_file = File.join(step_dir, "worker.rb")
          next unless File.exist?(worker_file)
          result[step_name] = resolve_step(worker_file, step_dir, project_root)
        end
        result
      end

      # Build a temporary directory containing external deps laid out by
      # project-relative path. This directory is passed as a BuildKit
      # --build-context and consumed via COPY --from=deps in the Dockerfile.
      #
      # Always returns a tmpdir (empty if no deps) so callers can
      # unconditionally pass --build-context deps=<dir>.
      #
      # Returns String — tmpdir path
      def self.prepare_build_context(external_deps, project_root)
        tmpdir = Dir.mktmpdir("turbofan-deps-")
        external_deps.each do |dep|
          relative = Pathname.new(dep)
            .relative_path_from(Pathname.new(project_root))
            .to_s
          dest = File.join(tmpdir, relative)
          FileUtils.mkdir_p(File.dirname(dest))
          FileUtils.cp(dep, dest)
        end
        tmpdir
      end

      # Remove the temporary build context directory.
      def self.cleanup_build_context(tmpdir)
        FileUtils.rm_rf(tmpdir) if tmpdir
      end

      # Load a single worker.rb in a forked process and return its external .rb deps.
      #
      # Fork isolates: class definitions, $LOADED_FEATURES changes, global state.
      # The parent process (which already loaded via PipelineLoader) is unaffected.
      def self.resolve_step(worker_file, step_dir, project_root)
        step_dir_expanded = File.expand_path(step_dir)
        project_root_expanded = File.expand_path(project_root)
        gem_prefixes = Gem.path.map { |p| File.join(p, "gems") }
        reader, writer = IO.pipe

        pid = fork do
          reader.close
          $LOAD_PATH.unshift(project_root_expanded) unless $LOAD_PATH.include?(project_root_expanded)

          # Clear external .rb files from $LOADED_FEATURES inherited from the
          # parent. PipelineLoader.load may have already loaded the worker and
          # its deps — if so, require would be a no-op and the before/after
          # diff would be empty. Clearing forces require to re-load them.
          $LOADED_FEATURES.reject! { |f|
            f.end_with?(".rb") &&
              !f.start_with?(step_dir_expanded) &&
              !gem_prefixes.any? { |gp| f.start_with?(gp) } &&
              !STDLIB_PREFIXES.any? { |sp| f.start_with?(sp) }
          }

          before = $LOADED_FEATURES.dup
          begin
            Kernel.load(File.expand_path(worker_file))
          rescue LoadError => e
            warn "[Turbofan] WARNING: Could not resolve deps for #{File.basename(step_dir)}: #{e.message}"
          end
          after = $LOADED_FEATURES

          deps = (after - before)
            .select { |f| f.end_with?(".rb") }
            .map { |f| File.expand_path(f) }
            .select { |f| File.exist?(f) }
            .reject { |f| f.start_with?(step_dir_expanded) }
            .reject { |f| gem_prefixes.any? { |gp| f.start_with?(gp) } }
            .reject { |f| STDLIB_PREFIXES.any? { |sp| f.start_with?(sp) } }

          writer.write(Marshal.dump(deps))
          writer.close
          exit!(0)
        end

        writer.close
        data = reader.read
        reader.close
        Process.wait(pid)

        return [] if data.empty?
        Marshal.load(data)
      rescue ArgumentError, TypeError => e
        warn "[Turbofan] WARNING: Failed to read deps for #{File.basename(step_dir)}: #{e.message}"
        []
      end
      private_class_method :resolve_step
    end
  end
end
