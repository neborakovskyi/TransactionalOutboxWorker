using Domain.Entities;

namespace Domain.Interfaces
{
    public interface IOutboxErrorClassifier
    {
        FailureKind Classify(Exception exception);
    }
}
