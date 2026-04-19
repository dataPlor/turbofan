# frozen_string_literal: true

module Turbofan
  class CLI < Thor
    module Prompt
      class << self
        attr_writer :input, :output

        def input = @input || $stdin
        def output = @output || $stdout
        def tty? = input.respond_to?(:tty?) && input.tty?

        def ask(question, default: nil)
          return default unless tty?
          output.print default ? "#{question} [#{default}]: " : "#{question}: "
          answer = input.gets&.chomp
          (answer.nil? || answer.empty?) ? default : answer
        end

        def yes?(question, default: true)
          return default unless tty?
          output.print "#{question} #{default ? "[Y/n]" : "[y/N]"} "
          answer = input.gets&.chomp&.downcase
          return default if answer.nil? || answer.empty?
          %w[y yes].include?(answer)
        end

        def select(question, choices, default: nil)
          default ||= choices.first
          return default unless tty?
          output.puts question
          choices.each_with_index { |c, i| output.puts "  #{i + 1}) #{c}" }
          output.print "Choice [#{choices.index(default).to_i + 1}]: "
          answer = input.gets&.chomp
          return default if answer.nil? || answer.empty?
          idx = answer.to_i - 1
          (idx >= 0 && idx < choices.size) ? choices[idx] : default
        end

        def confirm_destructive(message, expected_input:)
          return false unless tty?
          output.puts message
          output.print "Type '#{expected_input}' to confirm: "
          input.gets&.chomp == expected_input
        end

        def reset!
          @input = nil
          @output = nil
        end
      end
    end
  end
end
