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
        if items.size > Turbofan::Generators::ASL::MAX_ARRAY_SIZE
          write_chunked_inputs(items, s3_client:, bucket:, execution_id:, step_name:)
        else
          work = items.each_with_index.map { |item, index| [item, index] }
          threaded_work(work) do |item, index|
            s3_client.put_object(
              bucket: bucket,
              key: s3_key(execution_id, step_name, "input", "#{index}.json"),
              body: JSON.generate(item)
            )
          end
        end
      end

      def read_input(array_index:, s3_client:, bucket:, execution_id:, step_name:, chunk: nil)
        key = if chunk
          s3_key(execution_id, step_name, "input", chunk.to_s, "#{array_index}.json")
        else
          s3_key(execution_id, step_name, "input", "#{array_index}.json")
        end

        response = s3_client.get_object(bucket: bucket, key: key)
        JSON.parse(response.body.read)
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
              key = s3_key(execution_id, step_name, "output", chunk_key.to_s, "#{index}.json")
              response = s3_client.get_object(bucket: bucket, key: key)
              yield JSON.parse(response.body.read)
            end
          end
        else
          count.times do |index|
            key = s3_key(execution_id, step_name, "output", "#{index}.json")
            response = s3_client.get_object(bucket: bucket, key: key)
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
            response = s3_client.get_object(bucket: bucket, key: key)
            results[index] = JSON.parse(response.body.read)
          end
          results
        end
      end

      def write_chunked_inputs(items, s3_client:, bucket:, execution_id:, step_name:)
        max = Turbofan::Generators::ASL::MAX_ARRAY_SIZE
        work = items.each_with_index.map { |item, index| [item, index] }
        threaded_work(work) do |item, index|
          chunk = index / max
          local_index = index % max
          s3_client.put_object(
            bucket: bucket,
            key: s3_key(execution_id, step_name, "input", chunk.to_s, "#{local_index}.json"),
            body: JSON.generate(item)
          )
        end
      end
      private_class_method :write_chunked_inputs

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
          response = s3_client.get_object(bucket: bucket, key: key)
          results[ri] = JSON.parse(response.body.read)
        end
        results
      end
      private_class_method :collect_chunked_outputs

      # Processes an array of work items in parallel using a thread pool.
      # Each work item is an Array that gets splatted into the block.
      def threaded_work(work_items, &block)
        return if work_items.empty?

        queue = Queue.new
        work_items.each { |item| queue << item }
        errors = Queue.new

        thread_count = [work_items.size, THREAD_POOL_SIZE].min
        threads = Array.new(thread_count) do
          Thread.new do
            loop do
              item = begin
                queue.pop(true)
              rescue ThreadError
                break
              end
              begin
                yield(*item)
              rescue => e
                errors << e
              end
            end
          end
        end

        threads.each(&:join)
        return if errors.empty?

        all_errors = []
        all_errors << errors.pop until errors.empty?
        first = all_errors.first
        raise first if all_errors.size == 1

        raise first.class, "#{first.message} (and #{all_errors.size - 1} other error(s) in parallel work)"
      end
      private_class_method :threaded_work
    end
  end
end
