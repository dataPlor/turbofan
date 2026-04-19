# frozen_string_literal: true

class ScoreItemsRouter
  include Turbofan::Router

  sizes :s, :m, :l

  def route(item)
    item["__turbofan_size"] || :m
  end
end
