# frozen_string_literal: true

require 'json'
require 'aws-sdk-s3'

# Router files are only bundled into the per-step routed variant of this
# Lambda. The shared (non-routed) variant catches LoadError and runs without
# the Router module.
begin
  require_relative 'turbofan_router'
  require_relative 'router'
rescue LoadError
  # Non-routed variant — router not bundled in this Lambda zip.
end

S3 = Aws::S3::Client.new

def chunk(items, group_size)
  items.each_slice(group_size).to_a
end

def s3_key(*parts)
  prefix = ENV['TURBOFAN_BUCKET_PREFIX']
  key = parts.join('/')
  prefix && !prefix.empty? ? "#{prefix}/#{key}" : key
end

def read_items(event, bucket, execution_id)
  if event.key?('prev_step')
    prev_step = event['prev_step']
    if event.key?('prev_fan_out_parents')
      parents = event['prev_fan_out_parents']
      all_items = []
      parents.each do |parent|
        parent['size'].times do |idx|
          begin
            key = s3_key(execution_id, prev_step, 'output', "parent#{parent['index']}", "#{idx}.json")
            response = S3.get_object(bucket: bucket, key: key)
            all_items << JSON.parse(response.body.read)
          rescue Aws::S3::Errors::NoSuchKey
            nil # sentinel chunk — no output written, skip
          end
        end
      end
      all_items
    elsif event.key?('prev_fan_out_sizes')
      sizes = event['prev_fan_out_sizes']
      all_items = []
      sizes.each do |size_name, size_info|
        parents = size_info['parents'] || []
        parents.each do |parent|
          parent['size'].times do |idx|
            begin
              key = s3_key(execution_id, prev_step, 'output', size_name, "parent#{parent['index']}", "#{idx}.json")
              response = S3.get_object(bucket: bucket, key: key)
              all_items << JSON.parse(response.body.read)
            rescue Aws::S3::Errors::NoSuchKey
              nil # sentinel — skip
            end
          end
        end
      end
      all_items
    else
      key = s3_key(execution_id, prev_step, 'output.json')
      response = S3.get_object(bucket: bucket, key: key)
      data = JSON.parse(response.body.read)
      unless data.is_a?(Hash) && data.key?('items') && data['items'].is_a?(Array)
        raise "Invalid input from step '#{prev_step}': expected {\"items\": [...]} in output.json. " \
              "Got: #{data.is_a?(Hash) ? data.keys.inspect : data.class}"
      end
      data['items']
    end
  elsif event.key?('trigger')
    read_trigger_input(event['trigger'], bucket)
  else
    raise "No input source: expected 'prev_step' or 'trigger' in event"
  end
end

def read_trigger_input(input, bucket)
  unless input.is_a?(Hash) && input.key?('items_s3_uri')
    raise "Invalid trigger input for fan-out: expected {\"items_s3_uri\": \"s3://...\"}. " \
          "Fan-out items must be stored on S3 in {\"items\": [...]} format. " \
          "Got: #{input.class}"
  end

  uri = input['items_s3_uri']
  unless uri.is_a?(String) && uri.start_with?('s3://')
    raise "Invalid items_s3_uri: expected s3:// URI, got: #{uri.inspect}"
  end

  key = uri.sub("s3://#{bucket}/", '')
  if key == uri
    raise "items_s3_uri must reference the pipeline bucket (s3://#{bucket}/...). " \
          "Got: #{uri}"
  end

  response = S3.get_object(bucket: bucket, key: key)
  data = JSON.parse(response.body.read)

  unless data.is_a?(Hash) && data.key?('items') && data['items'].is_a?(Array)
    raise "Invalid S3 input format: expected {\"items\": [...]} in #{uri}. " \
          "Got: #{data.is_a?(Hash) ? data.keys.inspect : data.class}"
  end

  data['items']
end

MAX_ARRAY_SIZE = 10_000
MIN_ARRAY_SIZE = 2

def split_into_parents(chunks, execution_id, step_name, bucket, size_name: nil)
  parent_count = [(chunks.size.to_f / MAX_ARRAY_SIZE).ceil, 1].max
  base = chunks.size / parent_count
  remainder = chunks.size % parent_count

  parents = []
  offset = 0
  parent_count.times do |i|
    real_size = base + (i < remainder ? 1 : 0)
    parent_chunks = chunks[offset, real_size]

    # Batch requires ArrayProperties.Size >= 2. Pad with null sentinel.
    if parent_chunks.size < MIN_ARRAY_SIZE
      parent_chunks += [nil] * (MIN_ARRAY_SIZE - parent_chunks.size)
    end

    key = if size_name
      s3_key(execution_id, step_name, 'input', size_name, "parent#{i}", 'items.json')
    else
      s3_key(execution_id, step_name, 'input', "parent#{i}", 'items.json')
    end
    S3.put_object(bucket: bucket, key: key, body: JSON.generate(parent_chunks))
    parents << { 'index' => i, 'size' => [real_size, MIN_ARRAY_SIZE].max, 'real_size' => real_size }
    offset += real_size
  end

  parents
end

def handler(event:, context:)
  bucket = ENV['TURBOFAN_BUCKET']
  execution_id = event['execution_id'] || context.aws_request_id
  step_name = event['step_name']
  group_size = event['group_size']
  routed = event.fetch('routed', false)
  router_class_name = event['router_class']

  items = read_items(event, bucket, execution_id)

  if router_class_name
    unless defined?(Turbofan::Router)
      raise "router_class=#{router_class_name.inspect} requested but router.rb not bundled in this Lambda zip"
    end
    router = Object.const_get(router_class_name).new
    items = items.map do |item|
      item.merge('__turbofan_size' => router.route(item).to_s)
    end
  end

  if routed
    batch_sizes = event.fetch('batch_sizes', {})
    groups = {}
    items.each do |item|
      size = item.fetch('__turbofan_size', 'default')
      (groups[size] ||= []) << item
    end

    sizes = {}
    groups.each do |size_name, size_items|
      size_batch_size = batch_sizes.fetch(size_name, group_size)
      chunks = chunk(size_items, size_batch_size)
      parents = split_into_parents(
        chunks, execution_id, step_name, bucket, size_name: size_name
      )
      sizes[size_name] = { 'parents' => parents }
    end

    # Ensure all declared sizes are present (empty parents for sizes with no items)
    batch_sizes.each_key do |size_name|
      sizes[size_name] ||= { 'parents' => [] }
    end

    { 'sizes' => sizes }
  else
    chunks = chunk(items, group_size)
    parents = split_into_parents(chunks, execution_id, step_name, bucket)
    { 'parents' => parents }
  end
end
