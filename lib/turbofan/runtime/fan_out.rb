# frozen_string_literal: true

require "json"

module Turbofan
  module Runtime
    module FanOut
      THREAD_POOL_SIZE = 32

      module_function

      def s3_key(*parts)
        prefix = ENV["TURBOFAN_BUCKET_PREFIX"]
        key = parts.join("/")
        (prefix && !prefix.empty?) ? "#{prefix}/#{key}" : key
      end

      def write_inputs(items, s3_client:, bucket:, execution_id:, step_name:)
        return if items.empty?

        Turbofan::Retryable.call do
          s3_client.put_object(
            bucket: bucket,
            key: s3_key(execution_id, step_name, "input", "items.json"),
            body: JSON.generate(items)
          )
        end
      end

      def read_input(array_index:, s3_client:, bucket:, execution_id:, step_name:, chunk: nil, parent_index: nil)
        key = if chunk && parent_index
          s3_key(execution_id, step_name, "input", chunk.to_s, "parent#{parent_index}", "items.json")
        elsif chunk
          s3_key(execution_id, step_name, "input", chunk.to_s, "items.json")
        elsif parent_index
          s3_key(execution_id, step_name, "input", "parent#{parent_index}", "items.json")
        else
          s3_key(execution_id, step_name, "input", "items.json")
        end

        response = Turbofan::Retryable.call { s3_client.get_object(bucket: bucket, key: key) }
        JSON.parse(response.body.read)[array_index]
      end

      def each_output(s3_client:, bucket:, execution_id:, step_name:, count: nil, chunks: nil, &block)
        raise ArgumentError, "must provide either count or chunks" unless count || chunks

        unless block
          return enum_for(:each_output, s3_client: s3_client, bucket: bucket,
            execution_id: execution_id, step_name: step_name, count: count, chunks: chunks)
        end

        if chunks
          chunks.each do |chunk_key, chunk_count|
            chunk_count.times do |index|
              begin
                key = s3_key(execution_id, step_name, "output", chunk_key.to_s, "#{index}.json")
                # Retryable doesn't retry NoSuchKey — it propagates to the outer
                # rescue below for sentinel-skip (Batch minimum array padding).
                response = Turbofan::Retryable.call { s3_client.get_object(bucket: bucket, key: key) }
                yield JSON.parse(response.body.read)
              rescue Aws::S3::Errors::NoSuchKey
                nil # sentinel chunk — no output written, skip
              end
            end
          end
        else
          count.times do |index|
            key = s3_key(execution_id, step_name, "output", "#{index}.json")
            response = Turbofan::Retryable.call { s3_client.get_object(bucket: bucket, key: key) }
            yield JSON.parse(response.body.read)
          end
        end
      end

      def collect_outputs(s3_client:, bucket:, execution_id:, step_name:, count: nil, chunks: nil)
        if chunks
          collect_chunked_outputs(chunks, s3_client:, bucket:, execution_id:, step_name:)
        else
          raise ArgumentError, "collect_outputs requires either count: or chunks:" if count.nil?
          return [] if count == 0

          results = Array.new(count)
          work = Array.new(count) { |index| [index] }
          threaded_work(work) do |index|
            key = s3_key(execution_id, step_name, "output", "#{index}.json")
            response = Turbofan::Retryable.call { s3_client.get_object(bucket: bucket, key: key) }
            results[index] = JSON.parse(response.body.read)
          end
          results
        end
      end

      def collect_chunked_outputs(chunks, s3_client:, bucket:, execution_id:, step_name:)
        work = []
        result_index = 0
        chunks.each do |chunk, count|
          count.times do |index|
            work << [chunk, index, result_index]
            result_index += 1
          end
        end

        results = Array.new(work.size)
        threaded_work(work) do |chunk, index, ri|
          key = s3_key(execution_id, step_name, "output", chunk.to_s, "#{index}.json")
          begin
            # Retryable doesn't retry NoSuchKey — it propagates for sentinel-skip.
            response = Turbofan::Retryable.call { s3_client.get_object(bucket: bucket, key: key) }
            results[ri] = JSON.parse(response.body.read)
          rescue Aws::S3::Errors::NoSuchKey
            # Sentinel chunk (padding for Batch minimum array size) — no output written
            results[ri] = nil
          end
        end
        results.compact
      end
      private_class_method :collect_chunked_outputs

      # Wraps a single-worker failure with the work item that triggered it.
      # Preserves the original exception's backtrace so callers can trace the
      # root cause without losing which thread/work item raised.
      class WorkerError < Turbofan::Error
        attr_reader :work_item, :cause

        def initialize(work_item, cause)
          @work_item = work_item
          @cause = cause
          super("Worker failed for #{work_item.inspect}: #{cause.class}: #{cause.message}")
          set_backtrace(cause.backtrace) if cause.backtrace
        end
      end

      # Aggregates multiple worker failures from a single threaded_work run.
      # Exposes `#errors` (Array<WorkerError>) so callers can iterate all
      # failures instead of only seeing "first + N others".
      class WorkerErrors < Turbofan::Error
        attr_reader :errors

        def initialize(errors)
          @errors = errors
          summary = errors.first(3).map { |e| "#{e.work_item.inspect}: #{e.cause.class}" }.join("; ")
          more = errors.size > 3 ? " (and #{errors.size - 3} more)" : ""
          super("#{errors.size} worker(s) failed: #{summary}#{more}")
        end
      end

      # Processes an array of work items in parallel using a thread pool.
      # Each work item is an Array that gets splatted into the block.
      #
      # Failure modes:
      #   - Zero failures → returns normally
      #   - One failure → raises a WorkerError wrapping the original exception
      #     (preserves backtrace, adds work-item identifier)
      #   - Multiple failures → raises a WorkerErrors aggregating all of them
      #     (each individual error available via #errors)
      #
      # Early exit:
      #   When `Turbofan.config.fan_out_early_exit_threshold` is a positive
      #   Integer N, workers stop dequeuing new items after N _non-transient_
      #   errors accumulate. Items already in flight complete normally; items
      #   still in the queue are skipped. This guards against poison-pill
      #   work items that would otherwise fail every remaining child at
      #   non-zero S3 / Retryable cost.
      #
      #   Transient errors (AWS throttling, networking) do NOT count toward
      #   the threshold — a throttle storm fails every worker with a
      #   retry-able error, and aborting early would make the operator's
      #   "your burst is being throttled" scenario indistinguishable from
      #   "your code has a real bug." Mike Perham flagged this specifically.
      def threaded_work(work_items, &block)
        return if work_items.empty?

        queue = Queue.new
        work_items.each { |item| queue << item }
        errors = Queue.new

        threshold = Turbofan.config.fan_out_early_exit_threshold
        non_transient_count_mutex = Mutex.new
        non_transient_count = 0
        aborted = false

        thread_count = [work_items.size, THREAD_POOL_SIZE].min

        # Stall detection: per-worker heartbeat timestamps. A coordinator
        # thread wakes periodically and warns for any worker whose
        # heartbeat is older than Turbofan.config.worker_stall_seconds.
        # nil (default) = no coordinator, no overhead.
        stall_threshold = Turbofan.config.worker_stall_seconds
        heartbeats = stall_threshold ? Array.new(thread_count, nil) : nil
        heartbeat_items = stall_threshold ? Array.new(thread_count, nil) : nil
        coordinator_shutdown = false

        threads = Array.new(thread_count) do |worker_idx|
          Thread.new do
            loop do
              # Check abort flag before dequeuing. Once tripped, drain
              # the remaining queue without doing any work.
              break if aborted
              item = begin
                queue.pop(true)
              rescue ThreadError
                break
              end
              if heartbeats
                heartbeats[worker_idx] = Process.clock_gettime(Process::CLOCK_MONOTONIC)
                heartbeat_items[worker_idx] = item
              end
              begin
                yield(*item)
              rescue => e
                errors << WorkerError.new(item, e)
                # Non-transient failures count toward the early-exit
                # threshold. Uses Turbofan::Retryable.transient? as the
                # single source of truth for what "transient" means
                # (HTTP 408/429/5xx + curated code list).
                next if threshold.nil?
                next if Turbofan::Retryable.transient?(e)
                non_transient_count_mutex.synchronize do
                  non_transient_count += 1
                  aborted = true if non_transient_count >= threshold
                end
              ensure
                if heartbeats
                  heartbeats[worker_idx] = nil
                  heartbeat_items[worker_idx] = nil
                end
              end
            end
          end
        end

        coordinator = if stall_threshold
          # Wake at ~quarter of the threshold so stall warnings fire
          # proportionally: 0.25s wake for a 1s threshold, 15s wake for
          # a 60s threshold. Floor at 50ms to avoid CPU burn at very
          # small thresholds (mostly for test scenarios).
          coordinator_interval = [stall_threshold / 4.0, 0.05].max
          Thread.new do
            until coordinator_shutdown
              sleep coordinator_interval
              next if coordinator_shutdown
              now = Process.clock_gettime(Process::CLOCK_MONOTONIC)
              heartbeats.each_with_index do |started_at, idx|
                next unless started_at
                stalled_for = now - started_at
                next if stalled_for < stall_threshold
                warn("[Turbofan] WorkerStall: thread #{idx} has held #{heartbeat_items[idx].inspect} " \
                     "for #{stalled_for.round(1)}s without finishing (threshold: #{stall_threshold}s)")
              end
            end
          end
        end

        threads.each(&:join)
        coordinator_shutdown = true
        coordinator&.join
        return if errors.empty?

        all_errors = []
        all_errors << errors.pop until errors.empty?
        raise all_errors.first if all_errors.size == 1
        raise WorkerErrors.new(all_errors)
      end
      private_class_method :threaded_work
    end
  end
end
