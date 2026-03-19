"""Utility to split large JSONL files into smaller chunks by size."""

from pathlib import Path


def split_jsonl_by_size(input_file, output_dir, chunk_size_mb=512):
    """
    Split a large JSONL file into multiple smaller files by size.
    
    Each output file will be approximately chunk_size_mb in size.
    Files are named as: <input_stem>_part_001.jsonl, _part_002.jsonl, etc.
    
    Args:
        input_file: Path to input JSONL file
        output_dir: Directory to write output files
        chunk_size_mb: Target size per chunk in MB (default 512)
    """
    input_path = Path(input_file)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    chunk_size_bytes = chunk_size_mb * 1024 * 1024
    part_num = 1
    current_size = 0
    outfile = None

    def open_new_file(part):
        return open(output_path / f"{input_path.stem}_part_{part:03d}.jsonl", "wb")

    try:
        outfile = open_new_file(part_num)

        with open(input_path, "rb") as infile:
            for line in infile:
                line_size = len(line)

                if current_size + line_size > chunk_size_bytes and current_size > 0:
                    outfile.close()
                    part_num += 1
                    outfile = open_new_file(part_num)
                    current_size = 0

                outfile.write(line)
                current_size += line_size

    finally:
        if outfile and not outfile.closed:
            outfile.close()

    print(f"Done. Created {part_num} file(s) in {output_path}")


if __name__ == "__main__":
    # Example usage: Split listings.jsonl into 512MB chunks
    split_jsonl_by_size("../listings.jsonl", "split_output", chunk_size_mb=512)
