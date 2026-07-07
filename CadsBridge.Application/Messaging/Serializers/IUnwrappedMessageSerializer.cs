using CadsBridge.Application.Messaging.Models;

namespace CadsBridge.Application.Messaging.Serializers;

public interface IUnwrappedMessageSerializer<out T>
{
    T? Deserialize(UnwrappedMessage message);
}