from dataclasses import dataclass

from devrelay.application.contracts.base_sources import BaseMaterialization
from devrelay.application.contracts.workspace_recovery import WorkspaceLocation


@dataclass(frozen=True, slots=True, kw_only=True)
class EditableWorkspace:
    """Same-process handoff observation; base bytes are historical once editable.

    This value supplies no sandbox, current lease, launch or restart reuse rights.
    Its live scope remains owned by the caller and must stay open for later work.
    """

    materialization: BaseMaterialization
    location: WorkspaceLocation

    def __post_init__(self) -> None:
        if not isinstance(self.materialization, BaseMaterialization) or not isinstance(
            self.location, WorkspaceLocation
        ):
            raise ValueError("Editable workspace requires original live observations.")
        self.materialization.__post_init__()
        self.location.__post_init__()
        child = self.materialization.reservation.identity
        if (
            child.volume_serial_number != self.location.identity.volume_serial_number
            or child == self.location.identity
        ):
            raise ValueError("Editable workspace changed its original root pins.")
