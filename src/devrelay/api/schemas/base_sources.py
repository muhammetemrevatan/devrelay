from devrelay.application.contracts.base_source_delivery import (
    BaseSourceDelivery,
    encode_base_delivery,
)
from devrelay.application.ports.base_source import InvalidBaseSourceData


class BaseSourceDeliveryResponse:
    """Render the consumed neutral wire contract without another serialization shape."""

    @staticmethod
    def encode(delivery: BaseSourceDelivery) -> bytes:
        try:
            return encode_base_delivery(delivery)
        except ValueError, TypeError:
            raise InvalidBaseSourceData(
                "Base source delivery failed serialization."
            ) from None
