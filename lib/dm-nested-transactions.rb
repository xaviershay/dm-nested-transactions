# Hacks to get nested transactions in Postgres
# Not extensively tested, more a proof of concept
#
# It re-opens the existing Transaction class to add a check for whether
# we need a nested transaction or not, and adds a new NestedTransaction
# transaction primitive that issues savepoint commands rather than begin/commit.

require 'dm-core'
require 'data_objects'

module DataMapper
  module Resource
    def transaction(&block)
      self.class.transaction(&block)
    end
  end

  class Transaction
    # Overridden to allow nested transactions
    def connect_adapter(adapter)
      if @transaction_primitives.key?(adapter)
        raise "Already a primitive for adapter #{adapter}"
      end

      primitive = if adapter.current_transaction
        adapter.nested_transaction_primitive
      else
        adapter.transaction_primitive
      end

      @transaction_primitives[adapter] = validate_primitive(primitive)
    end
  end

  module NestedTransactions
    def nested_transaction_primitive
      DataObjects::NestedTransaction.create_for_uri(normalized_uri, current_connection, self)
    end
  end
end

module DataObjects
  class NestedTransaction < Transaction

    # The host name. Note, this relies on the host name being configured
    # and resolvable using DNS
    HOST = "#{Socket::gethostbyname(Socket::gethostname)[0]}" rescue "localhost"
    @@counter = 0

    # The connection object for this transaction - must have already had
    # a transaction begun on it
    attr_reader :connection
    # A unique ID for this transaction
    attr_reader :id

    def self.create_for_uri(uri, connection, adapter)
      uri = uri.is_a?(String) ? URI::parse(uri) : uri
      DataObjects::NestedTransaction.new(uri, connection, adapter)
    end

    #
    # Creates a NestedTransaction bound to an existing connection
    #
    def initialize(uri, connection, adapter)
      @adapter    = adapter
      @connection = connection
      @id = Digest::SHA256.hexdigest("#{HOST}:#{$$}:#{Time.now.to_f}:nested:#{@@counter += 1}")[0..10]
    end

    def begin_statement
      lambda{ |id| "SAVEPOINT \"#{id}\"" }
    end

    def commit_statement
      case @adapter.class.name
      when /oracle/i
        nil
      else
        lambda{ |id| "RELEASE SAVEPOINT \"#{id}\"" }
      end
    end

    def rollback_statement
      lambda{ |id| "ROLLBACK TO SAVEPOINT \"#{id}\"" }
    end

    def close
    end

    def begin
      connection.create_command(begin_statement.call(@id)).execute_non_query
    end

    def commit
      statement = commit_statement
      if statement
        connection.create_command(commit_statement.call(@id)).execute_non_query
      end
    end

    def rollback
      connection.create_command(rollback_statement.call(@id)).execute_non_query
    end
  end
end

