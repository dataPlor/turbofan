class ProcessRouter
  include Turbofan::Router

  sizes :s, :m

  def route(item)
    item["big"] ? :m : :s
  end
end
