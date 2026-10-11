from pydantic import BaseModel, ConfigDict

from devrelay.application.contracts.acknowledgements import AcknowledgementResult


class AcknowledgementPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, hide_input_in_errors=True)

    result: AcknowledgementResult
