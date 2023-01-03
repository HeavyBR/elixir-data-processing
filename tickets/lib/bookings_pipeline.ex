defmodule BookingsPipeline do
  use Broadway

  @producer BroadwayRabbitMQ.Producer

  @one_minute 1 |> :timer.minutes()

  @producer_config [
    queue: "bookings_queue",
    declare: [durable: true],
    on_failure: :reject_and_requeue
  ]

  def start_link(_args) do
    opts = [
      name: BookingsPipeline,
      producer: [
        module: {@producer, @producer_config},
        concurrency: System.schedulers_online()
      ],
      processors: [
        default: [
          concurrency: System.schedulers_online()
        ]
      ],
      batchers: [
        cinema: [
          batch_timeout: @one_minute
        ],
        musical: [
          batch_timeout: @one_minute
        ],
        default: [
          batch_timeout: @one_minute
        ]
      ]
    ]

    Broadway.start_link(__MODULE__, opts)
  end

  def handle_message(_processor, message, _context) do
    %{data: %{event: event}} = message

    if Tickets.tickets_avaiable?(event) do
      case message do
        %{data: %{event: "cinema"}} = message ->
          Broadway.Message.put_batcher(message, :cinema)

        %{data: %{event: "musical"}} = message ->
          Broadway.Message.put_batcher(message, :musical)

        message ->
          message
      end
    else
      Broadway.Message.failed(message, "bookings-closed")
    end
  end

  def prepare_messages(messages, _context) do
    messages =
      Enum.map(messages, fn message ->
        Broadway.Message.update_data(message, fn data ->
          [event, user_id] = String.split(data, ",")
          %{event: event, user_id: user_id}
        end)
      end)

    users = Tickets.users_by_ids(Enum.map(messages, & &1.data.user_id))

    Enum.map(messages, fn message ->
      Broadway.Message.update_data(message, fn data ->
        user = Enum.find(users, &(&1.id == data.user_id))
        Map.put(data, :user, user)
      end)
    end)
  end

  def handle_batch(_batcher, messages, batch_info, _context) do
    IO.inspect(batch_info, label: "#{inspect(self())} batch")

    messages
    |> Tickets.insert_all_tickets()
    |> Enum.each(fn message ->
      channel = message.metadata.amqp_channel
      payload = "email,#{message.data.user.email}"
      AMQP.Basic.publish(channel, "", "notifications_queue", payload)
    end)

    messages
  end

  def handle_failed(messages, _context) do
    Enum.map(messages, fn
      %{status: {:failed, "bookings-closed"}} = message ->
        Broadway.Message.configure_ack(message, on_failure: :reject)

      message ->
        message
    end)
  end
end
