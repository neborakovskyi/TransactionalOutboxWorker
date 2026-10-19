using System.Data.Common;
using Domain.Interfaces;
using Microsoft.Data.SqlClient;

namespace Outbox.Worker.Persistence;

public sealed class SqlConnectionFactory(string connectionString) : ISqlConnectionFactory
{

    public async Task<DbConnection> OpenAsync(CancellationToken cancellationToken)
    {
        // Создавать SqlConnection на каждый вызов нормально: физические соединения
        // переиспользуются пулом ADO.NET, объект SqlConnection дешёвый.
        var connection = new SqlConnection(connectionString);
        try
        {
            await connection.OpenAsync(cancellationToken);
            return connection;
        }
        catch
        {
            // Если открыть не удалось, освобождаем объект сами, иначе он утечёт.
            await connection.DisposeAsync();
            throw;
        }
    }
}