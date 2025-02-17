import os
from warcio.archiveiterator import ArchiveIterator

input_folder = "./extracted/"  # Change this to where your WARC files are
output_folder = "./extracted_html/"

os.makedirs(output_folder, exist_ok=True)

for filename in os.listdir(input_folder):
    if filename.endswith(".warc"):
        filepath = os.path.join(input_folder, filename)

        with open(filepath, "rb") as stream:
            for record in ArchiveIterator(stream):
                if record.rec_type == "response":
                    url = record.rec_headers.get_header("WARC-Target-URI")
                    content = record.content_stream().read().decode("utf-8", errors="ignore")

                    # Generate a safe filename based on URL
                    safe_filename = url.replace("https://", "").replace("http://", "").replace("/", "_") + ".html"

                    # Save extracted content
                    with open(os.path.join(output_folder, safe_filename), "w", encoding="utf-8") as f:
                        f.write(content)

print("Extraction complete! HTML files are in the 'extracted_html' folder.")

