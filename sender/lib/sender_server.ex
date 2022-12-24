defmodule SenderServer do
  require Logger
  use GenServer

  def init(args) do
    Logger.info("Received arguments: #{inspect(args)}")
    max_retries = Keyword.get(args, :max_retries, 5)
    state = %{emails: [], max_retries: max_retries}
    Process.send_after(self(), :retry, 5000)
    {:ok, state, {:continue, :fetch_from_database}}
  end

  def handle_call(:get_state, _from, state) do
    {:reply, state, state}
  end

  def handle_cast({:send, email}, state) do
    status = send_email_and_get_status(email)

    emails = [%{email: email, status: status, retries: 0}] ++ state[:emails]
    {:noreply, %{state | emails: emails}}
  end

  def handle_info(:retry, state) do
    {failed, done} =
      Enum.split_with(state[:emails], fn item ->
        item.status == "failed" && item.retries < state[:max_retries]
      end)

    retried =
      Enum.map(failed, fn item ->
        Logger.info("Retrying email #{item.email}...")

        new_status = send_email_and_get_status(item.email)

        %{email: item.email, status: new_status, retries: item.retries + 1}
      end)

    Process.send_after(self(), :retry, 5000)

    {:noreply, %{state | emails: retried ++ done}}
  end

  def handle_continue(:fetch_from_database, state) do
    Logger.info("Handle continue called")
    {:noreply, state}
  end

  defp send_email_and_get_status(email) do
    case Sender.send_email(email) do
      {:ok, "email_sent"} -> "sent"
      :error -> "failed"
    end
  end

  def terminate(reason, _state), do: Logger.info("Terminating with reason #{reason}")
end
