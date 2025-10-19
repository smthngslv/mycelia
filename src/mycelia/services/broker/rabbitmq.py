import functools
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any, ClassVar, Final, Self, final
from uuid import UUID, uuid4

import aio_pika
from aio_pika.abc import (
    AbstractChannel,
    AbstractConnection,
    AbstractExchange,
    AbstractIncomingMessage,
    AbstractQueue,
    DeliveryMode,
    ExchangeType,
)
from aio_pika.message import Message
from aiormq.abc import Basic, ConfirmationFrameType
from pydantic import Field

from mycelia.core.entities import EnqueuedNode
from mycelia.services.broker.interface import IBroker, OnNodeEnqueuedCallback, OnSessionCancelledCallback
from mycelia.tracing import TRACER, Tracer
from mycelia.utils import Entity

__all__: Final[tuple[str, ...]] = ("RabbitMQBroker", "RabbitMQBrokerParams")


@final
class RabbitMQBrokerParams(Entity):
    queue_name: str
    # TODO: Annotated syntax is not supported by mypy yet.
    queue_prefetch_count: int = Field(default=0, ge=0)
    queue_is_exclusive: bool = Field(default=False)
    queue_max_priority: int | None = Field(default=None, ge=1)
    event_priority: int | None = Field(default=None, ge=0)


@final
class RabbitMQBroker(IBroker[RabbitMQBrokerParams]):
    __slots__: Final[tuple[str, ...]] = ("__connection", "__consumers", "__control_exchange", "__node_exchange")

    __TRACER: ClassVar[Tracer] = TRACER.get_child("services.broker.rabbitmq")
    __NODE_EXCHANGE_NAME: ClassVar[str] = "node"
    __CONTROL_EXCHANGE_NAME: ClassVar[str] = "control"

    @classmethod
    def get_bytes_from_params(cls: type[Self], /, params: RabbitMQBrokerParams) -> bytes:
        return params.to_bytes()

    @classmethod
    def get_params_from_bytes(cls: type[Self], /, packed: bytes) -> RabbitMQBrokerParams:
        return RabbitMQBrokerParams.from_bytes(packed)

    @classmethod
    @__TRACER.with_span_async(__TRACER.INFO, "rabbitmq_broker.create")
    async def create(cls: type[Self], /, url: str) -> Self:
        connection: Final[AbstractConnection] = await aio_pika.connect(url)
        channel: Final[AbstractChannel] = await connection.channel()
        node_exchange: Final[AbstractExchange] = await channel.declare_exchange(
            cls.__NODE_EXCHANGE_NAME, ExchangeType.DIRECT, durable=True
        )
        control_exchange: Final[AbstractExchange] = await channel.declare_exchange(
            cls.__CONTROL_EXCHANGE_NAME, ExchangeType.FANOUT, durable=True
        )
        return cls(connection, node_exchange, control_exchange)

    def __init__(
        self: Self,
        /,
        connection: AbstractConnection,
        node_exchange: AbstractExchange,
        control_exchange: AbstractExchange,
    ) -> None:
        self.__connection: Final[AbstractConnection] = connection
        self.__node_exchange: Final[AbstractExchange] = node_exchange
        self.__control_exchange: Final[AbstractExchange] = control_exchange
        self.__consumers: Final[
            dict[OnNodeEnqueuedCallback | OnSessionCancelledCallback, tuple[AbstractQueue, str]]
        ] = {}

    @__TRACER.with_span_async(__TRACER.DEBUG, "rabbitmq_broker.publish_node_enqueued")
    async def publish_node_enqueued(self: Self, /, params: RabbitMQBrokerParams, node: EnqueuedNode) -> None:
        self.__TRACER.set_attributes_to_current_span(node=node, params=params)
        receipt: Final[ConfirmationFrameType | None] = await self.__node_exchange.publish(
            message=Message(
                body=node.id.bytes + node.session_id.bytes + node.trace_context,
                content_type="application/octet-stream",
                delivery_mode=DeliveryMode.PERSISTENT,
                priority=params.event_priority,
                message_id=str(uuid4()),
                timestamp=datetime.now(UTC),
                type="node.ready",
                app_id="mycelia",
            ),
            routing_key=params.queue_name,
        )

        if not isinstance(receipt, Basic.Ack):
            self.__TRACER.warning("rabbitmq_broker.publish_node_enqueued.not_queued", params=params, node=node)

    @__TRACER.with_span_async(__TRACER.DEBUG, "rabbitmq_broker.publish_session_cancelled")
    async def publish_session_cancelled(self: Self, /, id_: UUID) -> None:
        self.__TRACER.set_attributes_to_current_span(id=id_)
        receipt: Final[ConfirmationFrameType | None] = await self.__control_exchange.publish(
            message=Message(
                id_.bytes,
                content_type="application/octet-stream",
                delivery_mode=DeliveryMode.PERSISTENT,
                message_id=str(uuid4()),
                timestamp=datetime.now(UTC),
                type="session.cancelled",
                app_id="mycelia",
            ),
            routing_key="",
        )

        if not isinstance(receipt, Basic.Ack):
            self.__TRACER.warning("rabbitmq_broker.publish_session_cancelled.not_queued", id=id_)

    @__TRACER.with_span_async(__TRACER.INFO, "rabbitmq_broker.add_on_node_enqueued_callback")
    async def add_on_node_enqueued_callback(
        self: Self, /, params: RabbitMQBrokerParams, callback: OnNodeEnqueuedCallback
    ) -> None:
        self.__TRACER.set_attributes_to_current_span(callback=callback, params=params)

        queue: AbstractQueue
        consumer_tag: str
        queue, consumer_tag = await self.__declare_queue_and_add_consumer(
            params.queue_name,
            self.__node_exchange,
            functools.partial(self.__node_enqueued_callback_wrapper, callback),
            is_exclusive=params.queue_is_exclusive,
            prefetch_count=params.queue_prefetch_count,
            max_priority=params.queue_max_priority,
        )
        self.__TRACER.set_attributes_to_current_span(consumer_tag=consumer_tag)
        self.__consumers[callback] = queue, consumer_tag

    @__TRACER.with_span_async(__TRACER.INFO, "rabbitmq_broker.add_on_session_cancelled_callback")
    async def add_on_session_cancelled_callback(self: Self, /, callback: OnSessionCancelledCallback) -> None:
        queue_name: Final[str] = f"control-{uuid4()}"
        self.__TRACER.set_attributes_to_current_span(queue_name=queue_name, callback=callback)

        queue: AbstractQueue
        consumer_tag: str
        queue, consumer_tag = await self.__declare_queue_and_add_consumer(
            queue_name,
            self.__control_exchange,
            functools.partial(self.__session_cancelled_callback_wrapper, callback),
            is_exclusive=True,
        )
        self.__TRACER.set_attributes_to_current_span(consumer_tag=consumer_tag)
        self.__consumers[callback] = queue, consumer_tag

    @__TRACER.with_span_async(__TRACER.INFO, "rabbitmq_broker.remove_callback")
    async def remove_callback(self: Self, /, callback: OnNodeEnqueuedCallback | OnSessionCancelledCallback) -> None:
        self.__TRACER.set_attributes_to_current_span(callback=callback)
        queue: AbstractQueue
        consumer_tag: str
        queue, consumer_tag = self.__consumers.pop(callback)
        self.__TRACER.set_attributes_to_current_span(queue_name=queue.name, consumer_tag=consumer_tag)
        await queue.channel.close()

    @__TRACER.with_span_async(__TRACER.INFO, "rabbitmq_broker.shutdown")
    async def shutdown(self: Self, /) -> None:
        await self.__connection.close()

    @__TRACER.with_span_async(__TRACER.TRACE, "rabbitmq_broker.declare_queue_and_add_consumer")
    async def __declare_queue_and_add_consumer(  # noqa: PLR0913
        self: Self,
        /,
        queue_name: str,
        exchange: AbstractExchange,
        on_message_callback: Callable[[AbstractIncomingMessage], Awaitable],
        *,
        is_exclusive: bool = False,
        prefetch_count: int = 0,
        max_priority: int | None = None,
    ) -> tuple[AbstractQueue, str]:
        self.__TRACER.set_attributes_to_current_span(
            queue_name=queue_name, is_exclusive=is_exclusive, prefetch_count=prefetch_count, max_priority=max_priority
        )
        channel: Final[AbstractChannel] = await self.__connection.channel()

        arguments: Final[dict[str, Any]] = {}
        if max_priority is not None:
            arguments["x-max-priority"] = max_priority

        try:
            await channel.set_qos(prefetch_count)

            queue: Final[AbstractQueue] = await channel.declare_queue(
                queue_name, durable=not is_exclusive, exclusive=is_exclusive, arguments=arguments
            )
            await queue.bind(exchange)

            consumer_tag: Final[str] = await queue.consume(on_message_callback)

        except Exception:
            await channel.close()
            raise

        self.__TRACER.set_attributes_to_current_span(consumer_tag=consumer_tag)
        return queue, consumer_tag

    @__TRACER.with_span_async(__TRACER.TRACE, "rabbitmq_broker.__node_enqueued_callback_wrapper")
    async def __node_enqueued_callback_wrapper(
        self: Self, /, callback: OnNodeEnqueuedCallback, message: AbstractIncomingMessage
    ) -> None:
        self.__TRACER.set_attributes_to_current_span(meessage=message)

        try:
            await callback(
                EnqueuedNode(
                    id=UUID(bytes=message.body[:16]),
                    session_id=UUID(bytes=message.body[16:32]),
                    trace_context=message.body[32:],
                )
            )

        except BaseException as exception:
            # TODO: Should I requeue here?
            self.__TRACER.error("rabbitmq_broker.__node_enqueued_callback_wrapper.error", exception_=exception)

        finally:
            await message.ack()

    @__TRACER.with_span_async(__TRACER.TRACE, "rabbitmq_broker.__session_cancelled_callback_wrapper")
    async def __session_cancelled_callback_wrapper(
        self: Self, /, callback: OnSessionCancelledCallback, message: AbstractIncomingMessage
    ) -> None:
        self.__TRACER.set_attributes_to_current_span(meessage=message)

        try:
            await callback(UUID(bytes=message.body))

        except BaseException as exception:
            # TODO: Should I requeue here?
            self.__TRACER.error("rabbitmq_broker.__session_cancelled_callback_wrapper.error", exception_=exception)

        finally:
            await message.ack()
