from pathlib import Path

def split_jsonl_by_size_binary(input_file, output_dir, chunk_size_mb=512):
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

# Example usage
split_jsonl_by_size_binary("listings.jsonl", "split_output", chunk_size_mb=512)