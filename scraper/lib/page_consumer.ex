defmodule PageConsumer do
  use GenStage
  require Logger

  def start_link(args) do
    initial_state = []

    args =
      if Keyword.has_key?(args, :id) do
        args
      else
        Keyword.put(args, :id, random_consumer_id())
      end

    type = Keyword.get(args, :type)
    id = Keyword.get(args, :id)

    GenStage.start_link(__MODULE__, initial_state, name: via(id, type))
  end

  def init(initial_state) do
    Logger.info("PageConsumer init")
    opts = [max_demand: 1, min_demand: 0]
    {:consumer, initial_state, subscribe_to: [{PageProducer, opts}]}
  end

  @spec handle_events(any, any, any) :: {:noreply, [], any}
  def handle_events(events, _from, state) do
    Logger.info("PageConsumer received #{inspect(events)}")

    Enum.each(events, fn _page ->
      Scraper.work()
    end)

    {:noreply, [], state}
  end

  defp via(key, value) do
    {:via, Registry, {Scraper.ConsumerRegistry, key, value}}
  end

  defp random_consumer_id() do
    :crypto.strong_rand_bytes(5)
    |> Base.url_encode64(padding: false)
  end
end
