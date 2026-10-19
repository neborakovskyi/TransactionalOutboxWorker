using Project1.Web.Domain.CartAggregate;
using Project1.Web.Domain.GuestUserAggregate;
using Project1.Web.Domain.OrderAggregate;
using Project1.Web.Domain.ProductAggregate;
using Vogen;

namespace Project1.Web.Infrastructure.Data.Config;

[EfCoreConverter<ProductId>]
[EfCoreConverter<CartId>]
[EfCoreConverter<CartItemId>]
[EfCoreConverter<GuestUserId>]
[EfCoreConverter<OrderId>]
[EfCoreConverter<OrderItemId>]
[EfCoreConverter<Quantity>]
[EfCoreConverter<Price>]
internal partial class VogenEfCoreConverters;
