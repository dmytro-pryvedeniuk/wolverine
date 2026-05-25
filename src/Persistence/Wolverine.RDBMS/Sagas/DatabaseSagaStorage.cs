using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using Wolverine.Persistence.Sagas;

namespace Wolverine.RDBMS.Sagas;

public class DatabaseSagaStorage<TId, TSaga> : ISagaStorage<TId, TSaga> where TSaga : Saga
{
    private readonly DbConnection _connection;
    private readonly IDatabaseSagaSchema<TId, TSaga> _schema;
    private DbTransaction? _tx;

    public DatabaseSagaStorage(DbConnection connection, DbTransaction tx, IDatabaseSagaSchema<TId, TSaga> schema)
    {
        _connection = connection;
        _tx = tx;
        _schema = schema;
    }

    public Task InsertAsync(TSaga saga, CancellationToken cancellationToken)
    {
        return _schema.InsertAsync(saga, _tx!, cancellationToken);
    }

    public Task UpdateAsync(TSaga saga, CancellationToken cancellationToken)
    {
        return _schema.UpdateAsync(saga, _tx!, cancellationToken);
    }

    public Task DeleteAsync(TSaga saga, CancellationToken cancellationToken)
    {
        return _schema.DeleteAsync(saga, _tx!, cancellationToken);
    }

    public async Task SaveChangesAsync(CancellationToken cancellationToken)
    {
        try
        {
            await _tx!.CommitAsync(cancellationToken);
            _tx = null;
        }
        finally
        {
            await _connection.CloseAsync();
        }
    }

    [SuppressMessage("IDisposableAnalyzers.Correctness", "IDISP007:Don't dispose injected", Justification = "Injected resources are owned")]
    public async ValueTask DisposeAsync()
    {
        if (_tx != null)
            await _tx.DisposeAsync().ConfigureAwait(false);
        if (_connection != null)
            await _connection.DisposeAsync().ConfigureAwait(false);
    }

    public Task<TSaga?> LoadAsync(TId id, CancellationToken cancellationToken)
    {
        return _schema.LoadAsync(id, _tx!, cancellationToken);
    }
}