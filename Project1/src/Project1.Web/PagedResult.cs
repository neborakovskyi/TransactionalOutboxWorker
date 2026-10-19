namespace Project1.Web;

public record PagedResult<T>(
  IReadOnlyList<T> Items,
  int Page,
  int PerPage,
  int TotalCount,
  int TotalPages);
