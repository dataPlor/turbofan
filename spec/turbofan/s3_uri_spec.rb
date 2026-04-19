# frozen_string_literal: true

require "spec_helper"

RSpec.describe Turbofan::S3Uri do
  describe ".new" do
    it "parses a simple S3 URI" do
      uri = described_class.new("s3://my-bucket/path/to/key")
      expect(uri.bucket).to eq("my-bucket")
      expect(uri.key).to eq("path/to/key")
    end

    it "parses S3 URI with no key" do
      uri = described_class.new("s3://my-bucket")
      expect(uri.bucket).to eq("my-bucket")
      expect(uri.key).to eq("")
    end

    it "parses S3 URI with trailing slash" do
      uri = described_class.new("s3://my-bucket/")
      expect(uri.bucket).to eq("my-bucket")
      expect(uri.key).to eq("")
    end

    it "parses S3 URI with wildcard key" do
      uri = described_class.new("s3://my-bucket/prefix/*")
      expect(uri.bucket).to eq("my-bucket")
      expect(uri.key).to eq("prefix/*")
    end

    it "raises on non-string input" do
      expect { described_class.new(nil) }.to raise_error(ArgumentError, /Invalid S3 URI/)
    end

    it "raises on non-S3 URI" do
      expect { described_class.new("https://example.com") }.to raise_error(ArgumentError, /Invalid S3 URI/)
    end

    it "raises on empty string" do
      expect { described_class.new("") }.to raise_error(ArgumentError, /Invalid S3 URI/)
    end
  end

  describe "#to_bucket_arn" do
    it "returns the bucket ARN" do
      uri = described_class.new("s3://my-bucket/some/key")
      expect(uri.to_bucket_arn).to eq("arn:aws:s3:::my-bucket")
    end
  end

  describe "#to_object_arn" do
    it "appends /* when key is empty" do
      uri = described_class.new("s3://my-bucket")
      expect(uri.to_object_arn).to eq("arn:aws:s3:::my-bucket/*")
    end

    it "preserves wildcard in key" do
      uri = described_class.new("s3://my-bucket/prefix/*")
      expect(uri.to_object_arn).to eq("arn:aws:s3:::my-bucket/prefix/*")
    end

    it "appends * to non-wildcard key" do
      uri = described_class.new("s3://my-bucket/prefix/")
      expect(uri.to_object_arn).to eq("arn:aws:s3:::my-bucket/prefix/*")
    end

    it "appends * to key without trailing slash" do
      uri = described_class.new("s3://my-bucket/prefix")
      expect(uri.to_object_arn).to eq("arn:aws:s3:::my-bucket/prefix*")
    end
  end

  describe "#to_arns" do
    it "returns both bucket and object ARNs" do
      uri = described_class.new("s3://my-bucket/key")
      arns = uri.to_arns
      expect(arns).to eq(["arn:aws:s3:::my-bucket", "arn:aws:s3:::my-bucket/key*"])
    end
  end

  describe "#to_s" do
    it "reconstructs the URI" do
      uri = described_class.new("s3://my-bucket/some/key")
      expect(uri.to_s).to eq("s3://my-bucket/some/key")
    end
  end
end
