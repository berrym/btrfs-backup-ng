from typing import Any, Optional, Dict, List

class Endpoint:
    config: Dict[str, Any]

    def __init__(
        self, config: Optional[Dict[str, Any]] = None, **kwargs: Any
    ) -> None: ...
    def _normalize_path(self, val: Any) -> str: ...
    def send(
        self,
        snapshot: Any,
        parent: Optional[Any] = None,
        clones: Optional[List[Any]] = None,
    ) -> Any: ...
    def receive(
        self,
        stdin: Any,
        snapshot_name: str = ...,
        parent_name: Optional[str] = ...,
    ) -> Any: ...
    def list_snapshots(self, flush_cache: bool = False) -> List[Any]: ...
    def correspondent_of(self, snapshot: Any) -> Optional[Any]: ...
    def _load_subvolume_ids_into(self, snapshots: List[Any]) -> None: ...
