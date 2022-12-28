defmodule Scraper do
  def work() do
    1..100
    |> Enum.random()
    |> Process.sleep()
  end

  def online?(_url) do
    work()

    Enum.random([false, true, true])
  end
end
