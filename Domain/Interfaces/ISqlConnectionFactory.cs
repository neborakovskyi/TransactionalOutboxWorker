using System.Data.Common;

namespace Domain.Interfaces;

public interface ISqlConnectionFactory
{
    // Возвращает УЖЕ ОТКРЫТОЕ соединение. Вызывающий обязан освободить его
    // через await using, и тогда оно вернётся в пул, а не закроется физически.
    Task<DbConnection> OpenAsync(CancellationToken cancellationToken);
}