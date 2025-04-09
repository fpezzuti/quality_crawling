
import sys, os

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from utils.config import config
from utils.component import Component
from pyterrier_quality import QualCache

CHECKPOINTS = config.get('qscorer').get('checkpoints', None)

class QualityScorer(Component):  
    """
        Class of the quality scorer component.
        It loads quality scores from a checkpoint and provides a method to get the quality score of a document.
    """
    def __init__(self, checkpoint: str, collection: str = "cw22b", verbose: bool = True) -> None:
        """
            Constructor of the quality scorer
            
            Args:
                checkpoint: huggingface checkpoint
                collection: string identifier of the collection
                verbose: boolean flag to print log messages
        """
        Component.__init__(self, verbose)
        self.component_name = "QSCORER"
        self.checkpoint = CHECKPOINTS[checkpoint][collection]

        self.log(f"Initialising qscorer for {collection} collection, from checkpoint={checkpoint}.")

        self.load_qscores()
   
    def load_qscores(self) -> None:
        """
            Load quality scores from the checkpoint
        """

        self.log(f"Loading quality scores from checkpoint={self.checkpoint}.")
        print(f"Loading quality scores from checkpoint={self.checkpoint}.")
        cache = QualCache.from_url(self.checkpoint)

        docnos = cache.docnos()
        scores = cache.quality_scores()

        self.log(f"Loaded {len(scores)} quality scores.")
        self.log(f"Loaded {len(docnos)} docnos.")

        self.log(f"Creating docno2score mapping.")
        self.docno2score = {str(docno): float(score) for docno, score in zip(docnos, scores)}
        self.log("Done creating docno2score mapping.")

    def get_score(self, docid: str) -> float:
        """
            Get the quality score of a document

            Args:
                docid: string identifier of the document
        """
        return self.docno2score.get(docid, None)
        
    def log(self, msg: str) -> None:
        """
            Log a message
        """
        if self.verbose:
            print(f"[{self.component_name}]: {msg}")