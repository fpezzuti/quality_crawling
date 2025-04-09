import os
import gzip
import json
from io import BytesIO
from typing import Tuple
import random
from typing import Dict, Iterator

SEED = 42

# set random seed
random.seed(SEED)


def read_docids(docids_fpath: str) -> list:
    """
        Read the docids from a file and return them as a list.

        Args:
            docids_fpath: path to the file containing docids
    """
    docids = []
    with open(docids_fpath, "r") as f:
        for line in f:
            docid = line.strip()
            docids.append(docid)
    return docids

def yield_initial_seeds(initial_seeds_fpath: str, limit: int | None = None) -> Iterator[str]:
    """
        Yield at most limit initial seeds from a file.

        Args:
            initial_seeds_fpath: path to the file containing initial seeds
            limit: maximum number of seeds to yield
    """
    with open(initial_seeds_fpath, "r") as f:
        count = 0
        for line in f:
            url = line.strip()
            yield url
            count += 1
            if limit is not None and count >= limit:
                break

def read_json_gz(file_path: str) -> Iterator[Dict]:
    """
        Read a JSON object from a gzipped file.

        Args: 
            file_path: path to the gzipped file
    """
    data = []
    with gzip.open(file_path, 'rt', encoding='utf-8') as f:
        for line in f:
            stripped_line = line.strip()
            if not stripped_line:
                continue  # skip empty lines
            try:
                yield json.loads(stripped_line)  # parse JSON obj.
            except json.JSONDecodeError as e:
                print(f"Error decoding line: {stripped_line}\n{e}")
    return data

def decompress_gzip_data(compressed_data: bytes) -> bytes:
    """
        Decompresses a gzip compressed data.
        
        Args:
            compressed_data: compressed data to decompress
    """
    with gzip.GzipFile(fileobj=BytesIO(compressed_data), mode='rb') as f:
        return f.read()

def random_read_json_gz(file_path: str, start_offset: int, end_offset: int | None = None) -> Dict:
    """
        Read a JSON object from a gzipped file at a random offset.
        
        Args:
            file_path: path to the gzipped file
            start_offset: start offset to read from
            end_offset: end offset to read until
    """
    with open(file_path, 'rb') as gz_file:
        gz_file.seek(start_offset)
        read_length = None if end_offset is None else (end_offset - start_offset)
        compressed_document = gz_file.read(read_length)
        decompressed_line = decompress_gzip_data(compressed_document).decode('utf-8') 
     
        try:
            return json.loads(decompressed_line)
        except json.JSONDecodeError as e:
            return None   

def read_offsets(file_path: str, i: int) -> Tuple[int, int]:
    """
        Read offsets from a file and returns a tuple storing the start and end offsets.

        Args:
            file_path: path to the file containing offsets
            i: index of the offset to read

    """
    offset_dim = 10
    offset_size = offset_dim+1  # 10 digits + newline
    with open(file_path, 'r') as f:
        f.seek(i * offset_size)
        start_offset = int(f.read(offset_dim))
        end_offset = int(f.read(offset_dim)) if f.read(1) else None
    return start_offset, end_offset

def get_parts(id: str) -> Tuple[str, str, int]:
    """
        Get file path's parts given a clueweb id

        Args:
            id: clueweb document id

    """
    _, subdir, file_seq, doc_seq = id.split('-')
    return subdir, file_seq, int(doc_seq)


def navigate_to_id(dir_path: str, id: str) -> None:
    """
        Navigate to a directory with a given id

        Args:
            dir_path: path to the directory (inlinks, outlinks, etc.)
            id: clueweb document id
    """
    subdir, file_seq, doc_seq = get_parts(id)

    file_prefix = os.path.join(dir_path, subdir, f"{subdir}-{file_seq}")

    file_fpath = f"{file_prefix}.json.gz"
    offsets_fpath = f"{file_prefix}.offset"
    
    try:
        start_offset, end_offset = read_offsets(offsets_fpath, doc_seq)
    except Exception as e:
        raise RuntimeError(f"Error reading offsets from file={offsets_fpath}: {e}")

    try:
        return random_read_json_gz(file_fpath, start_offset, end_offset)
    except Exception as e:
        raise RuntimeError(f"Error reading from gzipped JSON file={file_fpath}: {e}")