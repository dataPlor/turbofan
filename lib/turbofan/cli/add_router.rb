require "fileutils"

module Turbofan
  class CLI < Thor
    module AddRouter
      def self.call(step_name)
        Dir.chdir(Turbofan::CLI.project_root) do
          class_name = Turbofan::Naming.pascal_case(step_name)
          step_dir = File.join("turbofans", "steps", step_name)
          router_dir = File.join(step_dir, "router")

          FileUtils.mkdir_p(router_dir)
          write_router(router_dir, class_name)
          write_gemfile(router_dir)
        end
      end

      def self.write_router(dir, class_name)
        File.write(File.join(dir, "router.rb"), <<~RUBY)
          class #{class_name}Router
            include Turbofan::Router

            # Declare sizes matching the step's size definitions
            sizes :s, :m, :l

            # Classify each item into a size. Return a size symbol.
            # This runs on Lambda — keep it fast and dependency-light.
            def route(input)
              :m
            end
          end
        RUBY
      end

      def self.write_gemfile(dir)
        File.write(File.join(dir, "Gemfile"), <<~RUBY)
          source "https://rubygems.org"
          gem "turbofan"
          # Keep lightweight — this runs on Lambda
        RUBY
      end
    end
  end
end
