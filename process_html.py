import os
import re
from lxml import etree

def fix_mismatched_tags(input_file):
    """
    Before stripping tags process HTML to ensure no mismatched tags

    :param input_file: filename to be processed
    :type input_file: list[str]
    :return: The ingredients list.
    :rtype: list[str]

    """
    with open(input_file, "r", encoding="utf-8") as f:
        content = f.read()

    fixed_content = re.sub(r'(<link\b[^>]*?)(?<!/)>', 
                           r'\1 />', content)
    return fixed_content

def process_html(input_file, output_file):
    try:
        print(f"Fixing mismatched tags in {input_file}...")
        fixed_content = fix_mismatched_tags(input_file)
        print("Parsing fixed content...")
        
        root = etree.HTML(fixed_content)
        
        head = root.find("head")
        if head is not None:
            for element in list(head):
                elem_str = etree.tostring(
                            element, 
                            pretty_print = True).decode().strip()
                
                if element.tag == "link":
                    rel = element.attrib.get("rel")
                    if rel == "stylesheet":
                    else:
                        head.remove(element)
                elif element.tag == "meta":
                    name = element.attrib.get("name")
                    if name != "cdc:last_published":
                        head.remove(element)
                else:
                    head.remove(element)
        else:
            print("Warning: No <head> tag found.")

        with open(output_file, "wb") as f:
            f.write(etree.tostring(root, pretty_print=True, 
                                    method="html", encoding="UTF-8"))
        print(f"Processed: {input_file} -> {output_file}")
    
    except Exception as e:
        print(f"Error processing {input_file}: {e}")

def process_directory(input_dir, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    
    for filename in os.listdir(input_dir):
        input_file = os.path.join(input_dir, filename)
        if os.path.isfile(input_file) and filename.endswith(".html"):
            output_file = os.path.join(output_dir, filename)
            process_html(input_file, output_file)

# Example usage:
input_directory = "public"   # Source directory
output_directory = "public/processed"  # Destination directory

process_directory(input_directory, output_directory)
