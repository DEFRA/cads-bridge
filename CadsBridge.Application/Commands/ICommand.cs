using MediatR;

namespace CadsBridge.Application.Commands;

public interface ICommand<out TResponse> : IRequest<TResponse> { }