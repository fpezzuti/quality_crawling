from dataclasses import dataclass, field
from typing import Optional, Dict, Any

@dataclass
class WebPage:
    id: Optional[str] = None
    url: Optional[str] = None
    docno: Optional[int] = None
    metadata: Optional[Dict[str, Any]] = field(default_factory=dict)

    def set_id(self, id: str) -> None:
        self.id = id

    def get_id(self) -> Optional[str]:
        return self.id
    
    def set_url(self, url: str) -> None:
        self.url = url

    def get_url(self) -> Optional[str]:
        return self.url
    
    def get_docno(self) -> Optional[int]:
        return self.docno
    
    def set_docno(self, docno: int) -> None:
        self.docno = docno
    

    def set_metadata(self, metadata: Dict[str, Any]) -> None:
        self.metadata = metadata
            
    def get_metadata(self, key: str, default: Any = None) -> Any:
        if self.metadata is None:
            raise ValueError(f"Error: Webpage's {self} metadata is None.")
        return self.metadata.get(key, default)
    
    def __str__(self) -> str:
        return f"WebPage(id={self.id}, url={self.url}, metadata={self.metadata})"
    
    def __repr__(self) -> str:
        return self.__str__()