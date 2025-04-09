
import sys, os

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from crawler.webpage import WebPage
from utils.config import config
from utils.utils import navigate_to_id
from utils.component import Component
from utils.qscorer import QualityScorer


COLLECTIONS = config.get('collections', None)

URL_INDEX = 0

MAX_QSIZE = 1000

SLEEP_INTERVAL = 0.5

QSCORER_CHECKPOINT = "qt5-small-ft"

class Parser(Component):
    """
        Class of the parser component.
        It parses the metadata of the downloaded pages, including outlinks, inlinks and quality score.
    """  
    def __init__(self, collection: str, to_parse: list =  [], verbose: bool = False) -> None:
        """
            Constructor of the parser
            
            Args:
                collection: string identifier of the collection
                to_parse: list of strings with the metadata to parse
                verbose: boolean flag to print log messages
        """
        Component.__init__(self, verbose)
        self.component_name = "PARSER"

        self.log(f"Initialising parser for {collection} collection.")
        self.to_parse = to_parse
        self.log(f"Metadata to parse={self.to_parse}.")

        collection_config = COLLECTIONS[collection]
        self.inlinks_dir = collection_config["inlinks_dir"]
        self.outlinks_dir = collection_config["outlinks_dir"]
       
        self.log(f"Out-Links will be read from directory={self.outlinks_dir}.")

        if "qscores" in self.to_parse:
            self.log(f"Initialising QScorer from checkpoint {QSCORER_CHECKPOINT}.")
            self.QScorer = QualityScorer(checkpoint=QSCORER_CHECKPOINT, collection=collection, verbose=True)

    def get_to_parse(self) -> list:
        """
            Return the list of metadata to parse
        """
        return self.to_parse
        
        
    def parse_metadata(self, page: WebPage) -> WebPage:
        """
            Update the WebPage object by parsing document's metadata given its WebPage object.

            Args:
                page: WebPage object to be updated
        """

        metadata = {}
        metadata["outlinks"] = self.__parse_outlinks(page)
        
        if "qscores" in self.to_parse:    
            qscore = self.__parse_qscore(page)
            metadata["qscore"] = qscore
        if "inlinks" in self.to_parse:
            inlinks = self.parse_num_inlinks(page)
            metadata["inlinks"] = inlinks

        page.set_metadata(metadata)
        return page
    

    def clean_links(self, links: list, url: str) -> list:
        """
            Remove self-links and external links (i.e., links that are not in the collection) from the list of links.

            Args:
                links: list of links to be cleaned
                url: url of the document that contains the links
        """
        cleaned_links = []

        for link in links:
            link_url = link[URL_INDEX]
            if (link_url != url) and (link_url not in cleaned_links):
                cleaned_links.append(link_url)
        return cleaned_links

    def __parse_outlinks(self, page: WebPage) -> list:
        """
            Extract the outlinks of a given WebPage object and return them as a list of strings.

            Args: 
                page: WebPage object of the page for which outlinks should be extracted
        """
        outlinks = []
        id = page.get_id()
        url = page.get_url()

        doc_data = navigate_to_id(self.outlinks_dir, id)
        if doc_data is None:
            return None
        raw_outlinks = doc_data['outlinks']

        if raw_outlinks is not None:
            outlinks = self.clean_links(raw_outlinks, url)

        return outlinks


    def parse_outlinks(self, page: WebPage) -> list:
        """
            Extract the outlinks of a given WebPage object and return them as a list of strings.
            This function wraps the internal implementation.

            Args:
                page: WebPage object of the page for which outlinks should be extracted
        """
        return self.__parse_outlinks(page)

    
    def __parse_inlinks(self, page: WebPage) -> float:
        """
            Extract the inlinks of a given WebPage object and return them as a list of strings.
            
            Args:
                page: WebPage object of the page for which inlinks should be extracted
        """
        inlinks = []
        docid = page.get_id()
        url = page.get_url()
       
        doc_data = navigate_to_id(self.inlinks_dir, docid)

        if doc_data is None:
            return []

        raw_inlinks = doc_data['anchors']

        if raw_inlinks is not None:
            inlinks = self.clean_links(raw_inlinks, url)
       
        return inlinks
        
    def parse_inlinks(self, page: WebPage) -> list:
        """
            Extract the inlinks of a given WebPage object and return them as a list of strings.
            This function wraps the internal implementation.

            Args:
                page: WebPage object of the page for which inlinks should be extracted
        """
        return self.__parse_inlinks(page)

    def __parse_numinlinks(self, page: WebPage) -> int:
        """
            Extract the number of inlinks of a given WebPage object and return it as an integer.
            
            Args:
                page: WebPage object of the page for which inlinks should be extracted
        """
        inlinks = self.__parse_inlinks(page)
        return len(inlinks) if (inlinks is not None) else 0
    
    def parse_num_inlinks(self, page: WebPage) -> int:
        """
            Extract the number of inlinks of a given WebPage object and return it as an integer.
            This function wraps the internal implementation.
        
            Args:
                page: WebPage object of the page for which inlinks should be extracted
        """
        return self.__parse_numinlinks(page)

    def __parse_qscore(self, page: WebPage) -> float:
        """
            Extract the quality score of a given WebPage object and return it as a float.

            Args:
                page: WebPage object of the page for which the quality score should be extracted
        """
        docid = page.get_id()
        return self.QScorer.get_score(docid)
    
    def parse_qscore(self, page: WebPage) -> float:
        """
            Extract the quality score of a given WebPage object and return it as a float.
            This function wraps the internal implementation.
        
            Args:
                page: WebPage object of the page for which the quality score should be extracted
        """
        return self.__parse_qscore(page)
        
    def log(self, msg: str) -> None:
        """
            Print a log message with the component name.
        """
        if self.verbose:
            print(f"[{self.component_name}]: {msg}")