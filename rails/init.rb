class NestedTransactionConfig < Rails::Railtie
  railtie_name :dm_nested_transactions

  config.after_initialize do
    repository.adapter.extend(DataMapper::NestedTransactions)
  end
end
