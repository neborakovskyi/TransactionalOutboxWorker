using Project1.Web.Domain.ProductAggregate;

namespace Project1.Web.ProductFeatures;

public record ProductDto(ProductId Id, string Name, decimal UnitPrice);
