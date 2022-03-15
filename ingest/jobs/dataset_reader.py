from pathlib import Path
from typing import Dict, List



from ingestor import (
    Attachment,
    Datablock,
    DataFile,
    Dataset,
    Issue,
    Ownable)

class DatasetReader():
    def __init__(self, folder) -> None:
        self._folder = folder

    def create_dataset(self, ownable: Ownable) -> Dataset:
        pass

    def create_data_block(self) -> Datablock:
        pass

    def create_data_files(self) -> List[DataFile]:
        pass

    # def create_ownable(self) -> Ownable:
    #     pass

    def create_scientific_metadata(self) -> Dict:
        pass

    def create_attachment(self, file: Path) -> Attachment:
        pass
